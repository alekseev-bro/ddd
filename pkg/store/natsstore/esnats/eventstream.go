package esnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/qos"

	"github.com/alekseev-bro/ddd/pkg/events"

	"math"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

const (
	maxAckPending int = 1000
)

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type eventStream[T any] struct {
	EventStreamConfig
	dedupe time.Duration
	// TODO: impl partitioning
	js jetstream.JetStream
}

func NewEventStream[T any](ctx context.Context, js jetstream.JetStream, cfg EventStreamConfig) *eventStream[T] {

	stream := &eventStream[T]{js: js, EventStreamConfig: cfg}

	_, err := stream.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Subjects:           []string{stream.allSubjects()},
		Name:               stream.streamName(),
		Storage:            jetstream.StorageType(stream.StoreType),
		Duplicates:         stream.dedupe,
		AllowDirect:        true,
		AllowAtomicPublish: true,
	})

	if err != nil {
		panic(err)
	}

	return stream
}

func (s *eventStream[T]) subjectNameForID(agrid string) string {
	return fmt.Sprintf("%s.%s", typereg.TypeNameFor[T](), agrid)
}
func (s *eventStream[T]) allSubjectsForID(agrid string) string {
	return fmt.Sprintf("%s.%s.>", typereg.TypeNameFor[T](), agrid)
}

func (s *eventStream[T]) streamName() string {
	return typereg.TypeNameFor[T]()
}

func (s *eventStream[T]) allSubjects() string {
	return fmt.Sprintf("%s.>", s.streamName())
}

func (s *eventStream[T]) Save(ctx context.Context, aggrID events.ID[T], msgs []*events.Event[T]) error {

	if msgs == nil {
		return nil
	}
	nmsgs := make([]*nats.Msg, len(msgs))
	for i, msg := range msgs {
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(aggrID.String()), msg.Kind)
		nmsg := nats.NewMsg(sub)
		b, err := serde.Serialize(msg.Body)
		if err != nil {
			slog.Error("command serialize", "error", err)
			panic(err)
		}
		nmsg.Data = b
		nmsg.Header.Add(jetstream.MsgIDHeader, msg.ID.String())
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqSubjHeader, s.allSubjectsForID(aggrID.String()))
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqHeader, strconv.Itoa(int(msg.PrevVersion)))
		nmsgs[i] = nmsg
	}

	if len(nmsgs) == 1 {
		ack, err := s.js.PublishMsg(ctx, nmsgs[0])
		if err != nil {
			var seqerr *jetstream.APIError
			if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				slog.Warn("occ", "version", msgs[0].PrevVersion, "name", s.subjectNameForID(aggrID.String()))
			}
			return fmt.Errorf("store event func: %w", err)
		}
		if ack.Duplicate {
			slog.Warn("duplicate event not stored", "kind", msgs[0].Kind, "subject", s.subjectNameForID(aggrID.String()), "stream", s.streamName())
			return nil
		}
		slog.Info("event stored", "kind", msgs[0].Kind, "subject", s.subjectNameForID(aggrID.String()), "stream", s.streamName())
		return nil
	}

	_, err := jetstreamext.PublishMsgBatch(ctx, s.js, nmsgs, jetstreamext.BatchFlowControl{AckEvery: 1, AckTimeout: time.Second})
	if err != nil {
		var seqerr *jetstream.APIError
		if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			slog.Warn("occ", "name", s.subjectNameForID(aggrID.String()))
		}
		return fmt.Errorf("save: %w", err)
	}

	for _, msg := range msgs {
		slog.Info("event stored", "kind", msg.Kind, "subject", s.subjectNameForID(aggrID.String()), "stream", s.streamName())
	}
	return nil
}

func (s *eventStream[T]) Load(ctx context.Context, id events.ID[T], version uint64) ([]*events.Event[T], error) {

	subj := s.allSubjectsForID(id.String())
	msgs, err := jetstreamext.GetBatch(ctx,
		s.js, s.streamName(), math.MaxInt, jetstreamext.GetBatchSubject(subj),
		jetstreamext.GetBatchSeq(version+1))
	//fmt.Println(time.Since(start))

	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	var evts []*events.Event[T]
	var prevEv *events.Event[T]
	for msg, err := range msgs {

		if err != nil {
			if errors.Is(err, jetstreamext.ErrNoMessages) {

				return nil, events.ErrNoAggregate
			}
			return nil, fmt.Errorf("build func can't get msg batch: %w", err)
		}

		event := eventFromMsg[T](jsRawMsgAdapter{msg})
		if prevEv == nil {
			event.PrevVersion = version
		} else {
			event.PrevVersion = prevEv.Version
		}

		prevEv = event
		evts = append(evts, event)
	}
	return evts, nil
}

type drainAdapter struct {
	jetstream.ConsumeContext
}

func (d *drainAdapter) Drain() error {
	d.ConsumeContext.Drain()
	return nil
}

type drainList []events.Drainer

func (d drainList) Drain() error {
	for _, drainer := range d {
		if err := drainer.Drain(); err != nil {
			return err
		}
	}
	return nil
}

func aggrIDFromParams[T any](params *events.SubscribeParams[T]) string {
	if params.AggrID != "" {
		return params.AggrID
	}

	return "*"
}

func (e *eventStream[T]) Subscribe(ctx context.Context, handler func(event *events.Event[T]) error, params *events.SubscribeParams[T]) (events.Drainer, error) {

	maxpend := maxAckPending
	if params.QoS.Ordering == qos.Ordered {
		maxpend = 1
	}

	var filter []string
	if params.Kind != nil {
		for _, kind := range params.Kind {
			filter = append(filter, fmt.Sprintf("%s.%s.%s", e.streamName(), aggrIDFromParams(params), kind))
		}
	} else {
		filter = append(filter, fmt.Sprintf("%s.%s.%s", e.streamName(), aggrIDFromParams(params), "*"))
	}
	if params.QoS.Delivery == qos.AtMostOnce {
		subs := make(drainList, len(filter))
		for i, f := range filter {
			sub, err := e.js.Conn().Subscribe(f, func(msg *nats.Msg) {

				var target *events.InvariantViolationError
				if err := handler(eventFromMsg[T](natsMessageAdapter{msg})); err != nil {
					if !errors.As(err, &target) {
						slog.Warn("redelivering", "error", err)
						msg.Nak()
						return
					} else {
						slog.Warn("invariant violation", "reason", err.Error())
					}
				}
				msg.Ack()

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
		// mt, err := msg.Metadata()
		// if err != nil {
		// 	slog.Error("subscription metadata", "error", err)
		// 	slog.Warn("redelivering")
		// 	msg.Nak()
		// 	return
		// }

		var target *events.InvariantViolationError
		if err := handler(eventFromMsg[T](natsJSMsgAdapter{msg})); err != nil {
			if !errors.As(err, &target) {
				slog.Warn("redelivering", "error", err)
				msg.Nak()
				return
			} else {
				slog.Warn("invariant violation", "reason", err.Error())
			}
		}
		msg.Ack()

	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {}))
	if err != nil {
		panic(fmt.Errorf("subscription consume: %w", err))
	}
	return drainList{&drainAdapter{ConsumeContext: ct}}, nil

}
