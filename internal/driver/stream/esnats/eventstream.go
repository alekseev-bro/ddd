package esnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/alekseev-bro/ddd/pkg/qos"

	"github.com/alekseev-bro/ddd/pkg/aggregate"

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

type eventStream struct {
	name string
	EventStreamConfig
	// TODO: impl partitioning
	js jetstream.JetStream
}

const (
	defaultDeduplication time.Duration = time.Minute * 2
)

func NewEventStream(ctx context.Context, js jetstream.JetStream, name string, cfg EventStreamConfig) *eventStream {
	if cfg.Deduplication == 0 {
		cfg.Deduplication = defaultDeduplication
	}
	stream := &eventStream{name: name, js: js, EventStreamConfig: cfg}

	_, err := stream.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Subjects:           []string{stream.allSubjects()},
		Name:               name,
		Storage:            jetstream.StorageType(cfg.StoreType),
		Duplicates:         cfg.Deduplication,
		AllowDirect:        true,
		AllowAtomicPublish: true,
	})

	if err != nil {
		panic(err)
	}

	return stream
}

func (s *eventStream) subjectNameForID(agrid string) string {
	return fmt.Sprintf("%s.%s", s.name, agrid)
}
func (s *eventStream) allSubjectsForID(agrid string) string {
	return fmt.Sprintf("%s.%s.>", s.name, agrid)
}

func (s *eventStream) allSubjects() string {
	return fmt.Sprintf("%s.>", s.name)
}

func (s *eventStream) Save(ctx context.Context, aggrID aggregate.ID, expectedSequence uint64, msgs []aggregate.Msg) ([]*aggregate.StoredMsg, error) {

	if msgs == nil {
		return nil, nil
	}
	nmsgs := make([]*nats.Msg, len(msgs))
	for i, msg := range msgs {
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(aggrID.String()), msg.Kind)
		nmsg := nats.NewMsg(sub)

		nmsg.Data = msg.Body
		nmsg.Header.Add(jetstream.MsgIDHeader, msg.ID.String())
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqSubjHeader, s.allSubjectsForID(aggrID.String()))
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqHeader, strconv.Itoa(int(expectedSequence)))
		nmsgs[i] = nmsg
	}

	if len(nmsgs) == 1 {
		ack, err := s.js.PublishMsg(ctx, nmsgs[0])
		if err != nil {
			var seqerr *jetstream.APIError
			if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				slog.Warn("occ", "version", expectedSequence, "name", s.subjectNameForID(aggrID.String()))
			}
			return nil, fmt.Errorf("store event func: %w", err)
		}
		if ack.Duplicate {
			slog.Warn("duplicate event not stored", "kind", msgs[0].Kind, "subject", s.subjectNameForID(aggrID.String()), "stream", s.name)
			return nil, nil
		}

		slog.Info("event stored", "kind", msgs[0].Kind, "subject", s.subjectNameForID(aggrID.String()), "stream", s.name)
		msgs := []*aggregate.StoredMsg{
			&aggregate.StoredMsg{
				Msg: aggregate.Msg{ID: aggrID, Kind: msgs[0].Kind}, Version: aggregate.Version{Sequence: ack.Sequence},
			}}

		return msgs, nil
	}

	batchAck, err := jetstreamext.PublishMsgBatch(ctx, s.js, nmsgs, jetstreamext.BatchFlowControl{AckEvery: 1, AckTimeout: time.Second})
	if err != nil {
		var seqerr *jetstream.APIError
		if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			slog.Warn("occ", "name", s.subjectNameForID(aggrID.String()))
		}
		return nil, fmt.Errorf("save: %w", err)
	}
	outmsgs := make([]*aggregate.StoredMsg, len(msgs))
	for i, msg := range msgs {

		outmsgs[i] = &aggregate.StoredMsg{
			Msg: aggregate.Msg{ID: aggrID, Kind: msgs[0].Kind}, Version: aggregate.Version{Sequence: batchAck.Sequence},
		}
		slog.Info("event stored", "kind", msg.Kind, "subject", s.subjectNameForID(aggrID.String()), "stream", s.name)
	}

	return outmsgs, nil
}

func (s *eventStream) Load(ctx context.Context, aggrID aggregate.ID, fromSeq uint64) ([]*aggregate.StoredMsg, error) {

	subj := s.allSubjectsForID(aggrID.String())
	msgs, err := jetstreamext.GetBatch(ctx,
		s.js, s.name, math.MaxInt, jetstreamext.GetBatchSubject(subj),
		jetstreamext.GetBatchSeq(fromSeq+1))
	//fmt.Println(time.Since(start))

	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	var evts []*aggregate.StoredMsg
	for msg, err := range msgs {

		if err != nil {
			if errors.Is(err, jetstreamext.ErrNoMessages) {

				return nil, aggregate.ErrNoAggregate
			}
			return nil, fmt.Errorf("build func can't get msg batch: %w", err)
		}

		event := eventFromMsg(jsRawMsgAdapter{msg})

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

type drainList []aggregate.Drainer

func (d drainList) Drain() error {
	for _, drainer := range d {
		if err := drainer.Drain(); err != nil {
			return err
		}
	}
	return nil
}

func aggrIDFromParams(params *aggregate.SubscribeParams) string {
	if params.AggrID != "" {
		return params.AggrID
	}

	return "*"
}

func (e *eventStream) Subscribe(ctx context.Context, handler func(msg *aggregate.StoredMsg) error, params *aggregate.SubscribeParams) (aggregate.Drainer, error) {

	maxpend := maxAckPending
	if params.QoS.Ordering == qos.Ordered {
		maxpend = 1
	}

	var filter []string
	if params.Kind != nil {
		for _, kind := range params.Kind {
			filter = append(filter, fmt.Sprintf("%s.%s.%s", e.name, aggrIDFromParams(params), kind))
		}
	} else {
		filter = append(filter, fmt.Sprintf("%s.%s.%s", e.name, aggrIDFromParams(params), "*"))
	}
	if params.QoS.Delivery == qos.AtMostOnce {
		subs := make(drainList, len(filter))
		for i, f := range filter {
			sub, err := e.js.Conn().Subscribe(f, func(msg *nats.Msg) {

				var target *aggregate.InvariantViolationError
				if err := handler(eventFromMsg(natsMessageAdapter{msg})); err != nil {
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
	cons, err := e.js.CreateOrUpdateConsumer(ctx, e.name, jetstream.ConsumerConfig{
		Durable:        params.DurableName,
		FilterSubjects: filter,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  maxpend,
		PriorityPolicy: jetstream.PriorityPolicyPinned,
		PriorityGroups: []string{"sub"},
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

		var target *aggregate.InvariantViolationError
		if err := handler(eventFromMsg(natsJSMsgAdapter{msg})); err != nil {
			if !errors.As(err, &target) {
				slog.Warn("redelivering", "error", err)
				msg.Nak()
				return
			} else {
				slog.Warn("invariant violation", "reason", err.Error())
			}
		}
		msg.Ack()

	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		slog.Error("subscription consume", "error", err)
	}), jetstream.PullPriorityGroup("sub"))
	if err != nil {
		panic(fmt.Errorf("subscription consume: %w", err))
	}
	return drainList{&drainAdapter{ConsumeContext: ct}}, nil

}
