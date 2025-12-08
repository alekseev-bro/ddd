package esnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/alekseev-bro/ddd/pkg/store"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore"

	"github.com/alekseev-bro/ddd/pkg/domain"

	"math"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

const (
	eventKindHeader string = "Event-Kind"
)

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type eventStream[T any] struct {
	storeType  StoreType
	partnum    uint8
	tname      string
	boundedCtx string
	js         jetstream.JetStream
}

func NewEventStream[T any](ctx context.Context, js jetstream.JetStream, opts ...option[T]) *eventStream[T] {
	aname, bcname := natsstore.MetaFromType[T]()

	stream := &eventStream[T]{js: js, tname: aname, boundedCtx: bcname}

	for _, opt := range opts {
		opt(stream)
	}

	_, err := stream.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Subjects:    []string{stream.allSubjects()},
		Name:        stream.streamName(),
		Storage:     jetstream.StorageType(stream.storeType),
		Duplicates:  2 * time.Minute,
		AllowDirect: true,
		RePublish: &jetstream.RePublish{
			Source:      stream.allSubjects(),
			Destination: stream.subscribeSubject(),
		},
	})

	if err != nil {
		panic(err)
	}

	return stream
}

func (s *eventStream[T]) subjectNameForID(agrid string) string {
	return fmt.Sprintf("%s:%s.%s", s.boundedCtx, s.tname, agrid)
}
func (s *eventStream[T]) allSubjectsForID(agrid string) string {
	return fmt.Sprintf("%s:%s.%s.>", s.boundedCtx, s.tname, agrid)
}

func (s *eventStream[T]) streamName() string {
	return fmt.Sprintf("%s:%s", s.boundedCtx, s.tname)
}

func (s *eventStream[T]) allSubjects() string {
	return fmt.Sprintf("%s.>", s.streamName())
}
func (s *eventStream[T]) subscribeSubject() string {
	return fmt.Sprintf("sub.%s.>", s.streamName())
}

func (s *eventStream[T]) Save(ctx context.Context, aggrID string, idempotencyKey string, events []*domain.Envelope) error {
	// TODO: handle many events
	for _, envel := range events {

		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(aggrID), envel.Kind)

		msg := nats.NewMsg(sub)

		msg.Header.Add(jetstream.MsgIDHeader, idempotencyKey)

		msg.Data = envel.Payload

		_, err := s.js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequenceForSubject(envel.Version, s.allSubjectsForID(aggrID)))
		if err != nil {
			var seqerr *jetstream.APIError
			if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				slog.Warn("occ", "version", envel.Version, "name", s.subjectNameForID(aggrID))
			}
			return fmt.Errorf("store event func: %w", err)
		}
		slog.Info("event stored", "kind", envel.Kind, "subject", s.subjectNameForID(aggrID), "stream", s.streamName())
		return nil
	}

	return nil
}

func msgID(h nats.Header) uuid.UUID {
	uup, err := uuid.Parse(h.Get(jetstream.MsgIDHeader))
	if err != nil {
		slog.Error("subscription uuid parse", "error", err, "value", jetstream.MsgIDHeader)
		panic(err)
	}
	return uup
}

func (s *eventStream[T]) Load(ctx context.Context, id string, version uint64, handler func(event *domain.Envelope)) (uint64, error) {
	var lastevent uint64
	subj := s.allSubjectsForID(id)
	msgs, err := jetstreamext.GetBatch(ctx,
		s.js, s.streamName(), math.MaxInt, jetstreamext.GetBatchSubject(subj),
		jetstreamext.GetBatchSeq(version+1))
	//fmt.Println(time.Since(start))

	if err != nil {
		return 0, fmt.Errorf("get events: %w", err)
	}

	for msg, err := range msgs {
		if err != nil {
			if errors.Is(err, jetstreamext.ErrNoMessages) {
				return 0, store.ErrNoAggregate
			}
			return 0, fmt.Errorf("build func can't get msg batch: %w", err)
		}
		subjectParts := strings.Split(msg.Subject, ".")

		envel := &domain.Envelope{
			ID:      msgID(msg.Header),
			Kind:    subjectParts[2],
			Version: msg.Sequence,
			Payload: msg.Data,
		}

		handler(envel)
		lastevent = msg.Sequence
	}
	return lastevent, nil
}

type drainAdapter struct {
	jetstream.ConsumeContext
}

func (d *drainAdapter) Drain() error {
	d.ConsumeContext.Drain()
	return nil
}

func (e *eventStream[T]) Subscribe(ctx context.Context, handler func(event *domain.Envelope) error, params *domain.SubscribeParams) ([]domain.Drainer, error) {

	maxpend := 1000
	if params.Ordering == domain.Ordered {
		maxpend = 1
	}
	var filter []string
	if params.Kind != nil {
		for _, kind := range params.Kind {
			filter = append(filter, fmt.Sprintf("%s.%s.%s", e.streamName(), params.AggrID(), kind))
		}
	} else {
		filter = append(filter, fmt.Sprintf("%s.%s.%s", e.streamName(), params.AggrID(), "*"))
	}
	if params.QoS == domain.AtMostOnce {
		subs := make([]domain.Drainer, len(filter))
		for i, f := range filter {
			sub, err := e.js.Conn().Subscribe(f, func(msg *nats.Msg) {
				seq, _ := strconv.Atoi(msg.Header.Get("Nats-Sequence"))
				subjectParts := strings.Split(msg.Subject, ".")
				handler(&domain.Envelope{
					ID:      msgID(msg.Header),
					Kind:    subjectParts[2],
					Version: uint64(seq),
					Payload: msg.Data,
				})
			})
			if err != nil {
				return nil, fmt.Errorf("at most once subscribe: %w", err)
			}
			subs[i] = sub
		}

		return subs, nil
	}
	cons, err := e.js.CreateOrUpdateConsumer(ctx, e.streamName(), jetstream.ConsumerConfig{
		Durable:        params.DurableName,
		FilterSubjects: filter,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  maxpend,
	})
	if err != nil {
		slog.Error("subscription create consumer", "error", err)
		panic(err)
	}
	ct, err := cons.Consume(func(msg jetstream.Msg) {
		mt, err := msg.Metadata()
		if err != nil {
			slog.Error("subscription metadata", "error", err)
			slog.Warn("redelivering", "error", err)
			msg.Nak()
			return
		}
		subjectParts := strings.Split(msg.Subject(), ".")
		envel := &domain.Envelope{
			ID:      msgID(msg.Headers()),
			Kind:    subjectParts[2],
			Version: mt.Sequence.Stream,
			Payload: msg.Data(),
		}
		if err := handler(envel); err != nil {
			slog.Warn("redelivering", "error", err)
			msg.Nak()
			return
		}
		msg.Ack()

	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {}))
	if err != nil {
		panic(fmt.Errorf("subscription consume: %w", err))
	}

	return []domain.Drainer{&drainAdapter{ConsumeContext: ct}}, nil

}
