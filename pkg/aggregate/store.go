package aggregate

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"fmt"

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

type Serder serde.Serder

// SetTypeEncoder sets the default encoder for the aggregate,
// encoder must be a Serder implementation.
// Default is JSON.
func SetTypeEncoder(s Serder) {
	serde.SetDefaultSerder(s)
}

// NewStore creates a new aggregate root using the provided event stream and snapshot store.
func NewStore[T Root, PT PRoot[T]](ctx context.Context, es EventStream[T], ss SnapshotStore, cfg AggregateConfig, opts ...RegisteredEvent[T, PT]) *store[T, PT] {

	if cfg.SnapthotMsgThreshold == 0 {
		cfg.SnapthotMsgThreshold = byte(snapshotSize)
	}
	if cfg.SnapshotMaxInterval == 0 {
		cfg.SnapshotMaxInterval = snapshotInterval
	}

	aggr := &store[T, PT]{
		AggregateConfig: cfg,
		es:              es,
		ss:              ss,
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
type Updater[T Root, PT PRoot[T]] interface {
	Update(ctx context.Context, id ID, modify func(state PT) (Events[T], error)) ([]*Event[T], error)
}

// type Creator[T Root] interface {
// 	Create(ctx context.Context, id ID, cms Executer[T]) ([]*Event[T], error)
// }

type CommandHandler[T Root, C any] interface {
	Handle(ctx context.Context, cmd C) ([]*Event[T], error)
}

type EventHandler[T Root, E Evolver[T]] interface {
	HandleEvent(ctx context.Context, event E) error
}

type EventsHandler[T Root] interface {
	HandleEvents(ctx context.Context, event Evolver[T]) error
}

// Subscriber is an interface that defines the Project method for projecting events on an aggregate.
// Events must implement the Event interface.
type Subscriber[T Root] interface {
	Subscribe(ctx context.Context, h EventsHandler[T], opts ...ProjOption[T]) (Drainer, error)
}

type Snapshot[T Root] struct {
	Aggregate *T
	Timestamp time.Time
}

// All aggregates must implement the Store interface.
type Store[T Root, PT PRoot[T]] interface {
	Subscriber[T]
	Updater[T, PT]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber[T Root] interface {
	Subscribe(ctx context.Context, handler func(envel *Event[T]) error, params *SubscribeParams[T]) (Drainer, error)
}

type ProjOption[T Root] func(p *SubscribeParams[T])

type EventStream[T Root] interface {
	Save(ctx context.Context, aggrID ID, msgs []*Event[T]) error
	Load(ctx context.Context, aggrID ID, fromSeq uint64) ([]*Event[T], error)
	subscriber[T]
}

type SnapshotStore interface {
	Save(ctx context.Context, key []byte, value []byte) error
	Load(ctx context.Context, key []byte) ([]byte, error)
}

// Aggregate store type it implements the Aggregate interface.
type PRoot[T Root] interface {
	*T
	VersionSetter
	Root
}

type store[T Root, PT PRoot[T]] struct {
	AggregateConfig
	es     EventStream[T]
	ss     SnapshotStore
	qos    qos.QoS
	pubsub *nats.Conn
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

func (a Aggregate) GuardExistance() error {
	if !a.Exists {
		return ErrAggregateNotExists
	}
	if a.Deleted {
		return ErrAggregateDeleted
	}
	return nil
}

func (a Aggregate) Version() Version {
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
		e.Body.Evolve(aggr)
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
	var state PT
	// if id.String() != idempKey {
	sn := new(Snapshot[T])
	b, err := a.ss.Load(ctx, []byte(id.String()))
	if err != nil {
		if !errors.Is(err, ErrNoSnapshot) {
			return nil, fmt.Errorf("build: %w", err)
		}
		sn = nil
	}

	if b != nil {
		if err := serde.Deserialize(b, sn); err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
	}

	state, err = a.Build(ctx, id, sn)
	if err != nil {
		return nil, fmt.Errorf("build aggrigate: %w", err)
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
		err = &InvariantViolationError{Err: err}
	}
	if evts == nil {
		return nil, err
	}
	numevents := len(evts)
	msgs := make([]*Event[T], numevents)
	for i, ev := range evts {

		// Panics if event isn't registered
		kind := typereg.GetKind(ev)

		msgs[i] = &Event[T]{
			ID:               ID(uuid.NewSHA1(id.UUID(), fmt.Appendf([]byte(idempKey), "%d", i))),
			ExpectedSequence: expVersion,
			Kind:             kind,
			Body:             ev,
		}
	}

	if err := a.es.Save(ctx, id, msgs); err != nil {
		return nil, fmt.Errorf("update save: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	//	if state != nil && state.needSnapshot(a.SnapthotMsgThreshold, a.SnapshotMaxInterval) {
	if numevents%int(a.SnapthotMsgThreshold) == 0 && time.Since(sn.Timestamp) > a.SnapshotMaxInterval {

		go func() {
			snap := &Snapshot[T]{
				Aggregate: state,
				Timestamp: time.Now(),
			}
			b, err := serde.Serialize(snap)
			if err != nil {
				slog.Error("snapshot serialize", "error", err)
				return
			}
			err = a.ss.Save(ctx, []byte(id.String()), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "sequence", state.Version().Sequence, "aggregateID", id.String(), "aggregate", reflect.TypeFor[T]().Name(), "timestamp", state.Version().Timestamp)

		}()

	}

	return msgs, err
}
