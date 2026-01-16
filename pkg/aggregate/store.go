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

// SetTypeEncoder sets the default encoder for the aggregate,
// encoder must be a Serder implementation.
// Default is JSON.

// NewStore creates a new aggregate root using the provided event stream and snapshot store.
func NewStore[T any, PT PRoot[T]](ctx context.Context, es EventStream[T], ss SnapshotStore, opts ...StoreOption[T, PT]) *store[T, PT] {

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

// Updater is an interface that defines the Update method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Executer interface.
type Updater[T any, PT PRoot[T]] interface {
	Update(ctx context.Context, id ID, modify func(state PT) (Events[T], error)) ([]*Event[T], error)
}

// type Creator[T Root] interface {
// 	Create(ctx context.Context, id ID, cms Executer[T]) ([]*Event[T], error)
// }

type CommandHandler[T any, C any] interface {
	Handle(ctx context.Context, cmd C) ([]*Event[T], error)
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

type Snapshot[T any] struct {
	Aggregate *T
	Timestamp time.Time
}

// All aggregates must implement the Store interface.
type Store[T any, PT PRoot[T]] interface {
	Subscriber[T]
	Updater[T, PT]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber interface {
	Subscribe(ctx context.Context, handler func(msg *StoredMsg) error, params *SubscribeParams) (Drainer, error)
}

type ProjOption func(p *SubscribeParams)

type Msg struct {
	ID   ID
	Body []byte
	Kind string
}

type StoredMsg struct {
	Msg
	Version
}

type EventStream[T any] interface {
	Save(ctx context.Context, aggrID ID, expectedVersion uint64, msgs []Msg) ([]*StoredMsg, error)
	Load(ctx context.Context, aggrID ID, fromSeq uint64) ([]*StoredMsg, error)
	subscriber
}

type SnapshotStore interface {
	Save(ctx context.Context, key []byte, value []byte) error
	Load(ctx context.Context, key []byte) ([]byte, error)
}

// Aggregate store type it implements the Aggregate interface.
type PRoot[T any] interface {
	*T
	VersionSetter
	Root
}

type typeRegistry interface {
	Register(tname string, c func() any)
	Create(name string) (any, error)
	NameFor(in any) (string, error)
}

type store[T any, PT PRoot[T]] struct {
	storeConfig
	es            EventStream[T]
	ss            SnapshotStore
	qos           qos.QoS
	pubsub        *nats.Conn
	snapshotCodec codec.Codec
	eventSerder   EventSerder[T]
	eventRegistry typeRegistry
}

type Version struct {
	Sequence  uint64
	Timestamp time.Time
}

type VersionSetter interface {
	setVersion(v Version)
}

type Root interface {
	Version() Version
	GuardExistance() error
}

type Aggregate struct {
	ID      ID
	version Version
	Exists  bool
	Deleted bool
}

func (a *Aggregate) GuardExistance() error {
	if !a.Exists {
		return ErrAggregateNotExists
	}
	if a.Deleted {
		return ErrAggregateDeleted
	}
	return nil
}

func (a *Aggregate) Version() Version {
	return a.version
}

func (a *Aggregate) setVersion(v Version) {
	a.version = v
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

func (a *store[T, PT]) Build(ctx context.Context, id ID, sn *Snapshot[T]) (PT, error) {
	aggr := PT(new(T))

	if sn != nil {
		aggr = sn.Aggregate
	}

	events, err := a.es.Load(ctx, id, aggr.Version().Sequence)
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
		ev.Evolve(aggr)
		aggr.setVersion(e.Version)
	}

	return aggr, nil
}

// func (a *store[T, PT]) Create(ctx context.Context, id ID, cmd Executer[T]) ([]*Event[T], error) {
// 	return a.Update(ctx, id, cmd, id.String())
// }

// Update executes a command on the aggregate root.
func (a *store[T, PT]) Update(
	ctx context.Context, id ID,
	modify func(state PT) (Events[T], error),

) ([]*Event[T], error) {
	idempKey := idempotanceKeyFromCtxOrRandom(ctx)
	var err error
	var invError error
	var state PT
	// if id.String() != idempKey {
	sn := new(Snapshot[T])
	b, err := a.ss.Load(ctx, []byte(id.String()))
	if err != nil {
		if !errors.Is(err, ErrNoSnapshot) {
			return nil, fmt.Errorf("update: %w", err)
		}
		sn = nil
	}

	if b != nil {
		if err := a.snapshotCodec.Unmarshal(b, sn); err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}
	}

	state, err = a.Build(ctx, id, sn)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	//	}
	var evts Events[T]
	var expVersion uint64
	if state != nil {
		evts, err = modify(state)
		expVersion = state.Version().Sequence
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
	msgs := make([]Msg, numevents)
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
		msgs[i] = Msg{
			ID:   ID(uuid.NewSHA1(id.UUID(), fmt.Appendf([]byte(idempKey), "%d", i))),
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
		events[i] = &Event[T]{
			ID:      ID(msg.ID),
			Kind:    msg.Kind,
			Body:    evts[i],
			Version: msg.Version.Sequence,
		}
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	//	if state != nil && state.needSnapshot(a.SnapthotMsgThreshold, a.SnapshotMaxInterval) {
	if numevents%int(a.SnapthotMsgThreshold) == 0 && time.Since(sn.Timestamp) > a.SnapshotMaxInterval {

		go func() {
			snap := &Snapshot[T]{
				Aggregate: state,
				Timestamp: time.Now(),
			}
			b, err := a.snapshotCodec.Marshal(snap)
			if err != nil {
				slog.Error("snapshot serialize", "error", err)
				panic(err)
			}
			err = a.ss.Save(ctx, []byte(id.String()), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "sequence", state.Version().Sequence, "aggregateID", id.String(), "aggregate", reflect.TypeFor[T]().Name(), "timestamp", state.Version().Timestamp)

		}()

	}

	return events, invError
}
