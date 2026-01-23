package aggregate

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"fmt"

	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/qos"
	"github.com/alekseev-bro/ddd/pkg/repo"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typereg"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// var nc *nats.Conn

type messageCount uint

const (
	snapshotSize      messageCount  = 100
	snapshotInterval  time.Duration = time.Second * 1
	idempotancyWindow time.Duration = time.Minute * 2
)

type InvariantViolationError struct {
	Err error
}

func (e InvariantViolationError) Error() string {
	return e.Err.Error()
}

type EventSerder[T any] interface {
	Serialize(Evolver[T]) ([]byte, error)
	Deserialize(string, []byte) (Evolver[T], error)
}

// NewStore creates a new aggregate root using the provided event stream and snapshot store.
func NewStore[T any, PT PRoot[T]](ctx context.Context, es eventStream, ss snapshotStore, opts ...StoreOption[T, PT]) *store[T, PT] {

	eventReg := typereg.New()
	eventCodec := codec.JSON
	eventSerder := serde.NewSerder[Evolver[T]](eventReg, eventCodec)

	aggr := &store[T, PT]{
		storeConfig: storeConfig{
			SnapthotMsgThreshold: byte(snapshotSize),
			SnapshotMaxInterval:  snapshotInterval,
		},
		es:            es,
		ss:            ss,
		eventSerder:   eventSerder,
		eventRegistry: eventReg,
		snapshotCodec: codec.JSON,
	}

	for _, o := range opts {
		o(aggr)
	}
	//	var zero T

	return aggr
}

// Mutator is an interface that defines the Update method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Executer interface.
type Mutator[T any, PT PRoot[T]] interface {
	Mutate(ctx context.Context, id ID, modify func(state PT) (Events[T], error)) ([]*Event[T], error)
}

// type Creator[T Root] interface {
// 	Create(ctx context.Context, id ID, cms Executer[T]) ([]*Event[T], error)
// }

type CommandHandler[T any, C any] interface {
	HandleCommand(ctx context.Context, cmd C) ([]*Event[T], error)
}

type EventHandler[T any, E Evolver[T]] interface {
	HandleEvent(ctx context.Context, event E) error
}

type EventsHandler[T any] interface {
	HandleEvents(ctx context.Context, event Evolver[T]) error
}

// Subscriber is an interface that defines the Project method for projecting events on an aggregate.
// Events must implement the Event interface.
type Subscriber[T any] interface {
	Subscribe(ctx context.Context, h EventsHandler[T], opts ...ProjOption) (Drainer, error)
}

// All aggregates must implement the Store interface.
type Store[T any, PT PRoot[T]] interface {
	Subscriber[T]
	Mutator[T, PT]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber interface {
	Subscribe(ctx context.Context, handler func(msg *repo.StoredMsg) error, params *SubscribeParams) (Drainer, error)
}

type ProjOption func(p *SubscribeParams)

type eventStream interface {
	Save(ctx context.Context, aggrID ID, expectedVersion uint64, msgs []repo.Msg) ([]*repo.StoredMsg, error)
	Load(ctx context.Context, aggrID ID, fromSeq uint64) ([]*repo.StoredMsg, error)
	subscriber
}

type snapshotStore interface {
	Save(ctx context.Context, key []byte, value []byte) error
	Load(ctx context.Context, key []byte) (*repo.Snapshot, error)
}

// Aggregate store type it implements the Aggregate interface.
type PRoot[T any] interface {
	*T
}

type typeRegistry interface {
	Register(tname string, c func() any)
	Create(name string) (any, error)
	NameFor(in any) (string, error)
}

type store[T any, PT PRoot[T]] struct {
	storeConfig
	es            eventStream
	ss            snapshotStore
	qos           qos.QoS
	pubsub        *nats.Conn
	snapshotCodec codec.Codec
	eventSerder   EventSerder[T]
	eventRegistry typeRegistry
}

type aggregate[T any] struct {
	ID        ID
	Sequence  uint64
	Timestamp time.Time
	State     T
}

// func (a *Aggregate) needSnapshot(maxmsgs byte, maxinterval time.Duration) bool {
// 	if len(a.events)%int(maxmsgs) == 0 && time.Since(a.SnapshotTime) > maxinterval {
// 		return true
// 	}
// 	return false
// }

// func (a *Aggregate) Apply(ev Evolver[T]) {
// 	ev.Evolve(a.State)
// 	a.events = append(a.events, ev)
// }

func (a *store[T, PT]) build(ctx context.Context, id ID, sn *repo.Snapshot) (*aggregate[PT], error) {
	aggr := &aggregate[PT]{
		State: PT(new(T)),
	}

	if sn != nil {
		var body aggregate[PT]
		if err := a.snapshotCodec.Unmarshal(sn.Body, &body); err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
		aggr = &body
	}

	events, err := a.es.Load(ctx, id, aggr.Sequence)
	if err != nil {
		if errors.Is(err, ErrNoAggregate) {
			if sn == nil {
				return nil, nil
			}
		} else {
			return nil, fmt.Errorf("buid %w", err)
		}
	}

	for _, e := range events {

		ev, err := a.eventSerder.Deserialize(e.Kind, e.Body)
		if err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
		ev.Evolve(aggr.State)
		aggr.Sequence = e.Sequence
	}

	return aggr, nil
}

// func (a *store[T, PT]) Create(ctx context.Context, id ID, cmd Executer[T]) ([]*Event[T], error) {
// 	return a.Update(ctx, id, cmd, id.String())
// }

// Update executes a command on the aggregate root.
func (a *store[T, PT]) Mutate(
	ctx context.Context, id ID,
	modify func(state PT) (Events[T], error),

) ([]*Event[T], error) {
	idempKey := idempotanceKeyFromCtxOrRandom(ctx)
	var err error
	var invError error
	var aggr *aggregate[PT]
	sn := new(repo.Snapshot)
	sn, err = a.ss.Load(ctx, []byte(id.String()))
	if err != nil {
		if !errors.Is(err, ErrNoSnapshot) {
			return nil, fmt.Errorf("update: %w", err)
		}
		sn = nil
	}

	aggr, err = a.build(ctx, id, sn)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	//	}
	var evts Events[T]
	var expVersion uint64
	if aggr != nil {
		evts, err = modify(aggr.State)
		expVersion = aggr.Sequence
	} else {
		evts, err = modify(new(T))
	}
	if err != nil {
		invError = &InvariantViolationError{Err: err}
	}
	if evts == nil {
		return nil, invError
	}
	numevents := len(evts)
	msgs := make([]repo.Msg, numevents)
	for i, ev := range evts {

		kind, err := a.eventRegistry.NameFor(ev)
		if err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}

		b, err := a.eventSerder.Serialize(ev)
		if err != nil {
			slog.Error("serialize failed", "error", err)
			panic(err)

		}
		msgs[i] = repo.Msg{
			ID:   uuid.NewSHA1(id.UUID(), fmt.Appendf([]byte(idempKey), "%d", i)).String(),
			Kind: kind,
			Body: b,
		}
	}

	storedMsgs, err := a.es.Save(ctx, id, expVersion, msgs)
	if err != nil {
		return nil, fmt.Errorf("update save: %w", err)
	}
	events := make([]*Event[T], len(storedMsgs))

	for i, msg := range storedMsgs {
		parid, err := uuid.Parse(msg.ID)
		if err != nil {
			panic(err)
		}
		events[i] = &Event[T]{
			ID:       ID(parid),
			Kind:     msg.Kind,
			Body:     evts[i],
			Sequence: msg.Sequence,
		}
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	//	if state != nil && state.needSnapshot(a.SnapthotMsgThreshold, a.SnapshotMaxInterval) {
	if numevents%int(a.SnapthotMsgThreshold) == 0 && time.Since(sn.Timestamp) > a.SnapshotMaxInterval {

		go func() {
			b, err := a.snapshotCodec.Marshal(aggr)
			err = a.ss.Save(ctx, []byte(id.String()), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "sequence", aggr.Sequence, "aggregateID", id.String(), "aggregate", reflect.TypeFor[T]().Name(), "timestamp", aggr.Timestamp)

		}()
	}

	return events, invError
}
