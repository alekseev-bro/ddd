package essrv

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"fmt"

	"github.com/alekseev-bro/ddd/pkg/qos"
	"github.com/alekseev-bro/ddd/pkg/store"

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

var amu sync.RWMutex
var aggregates map[reflect.Type]any = make(map[reflect.Type]any)

func getAggregate[T any]() (*root[T], bool) {
	amu.RLock()
	defer amu.RUnlock()

	if agg, ok := aggregates[reflect.TypeFor[T]()]; ok {
		return agg.(*root[T]), true
	}
	return nil, false
}

func setAggregate[T any](agg *root[T]) {
	amu.Lock()
	defer amu.Unlock()

	aggregates[reflect.TypeFor[T]()] = agg
}

// AggregateNameFromType returns the aggregate name and bounded context name from the type T.
func AggregateNameFromType[T any]() (aname string, bctx string) {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		panic("T must be a struct")
	}

	aname = t.Name()
	sep := strings.Split(t.PkgPath(), "/")
	bctx = sep[len(sep)-1]
	return
}

type Serder serde.Serder

func (*root[T]) NewID() ID[T] {
	a, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return ID[T](a)
}

// SetTypeEncoder sets the default encoder for the aggregate,
// encoder must be a Serder implementation.
// Default is JSON.
func SetTypeEncoder(s Serder) {
	serde.SetDefaultSerder(s)
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[T any](ctx context.Context, es EventStream[T], ss SnapshotStore[T], cfg AggregateConfig, opts ...RegisteredEvent[T]) *root[T] {
	if aggr, ok := getAggregate[T](); ok {
		return aggr
	}

	if cfg.SnapthotMsgThreshold == 0 {
		cfg.SnapthotMsgThreshold = byte(snapshotSize)
	}
	if cfg.SnapshotMaxInterval == 0 {
		cfg.SnapshotMaxInterval = snapshotInterval
	}

	aggr := &root[T]{
		AggregateConfig: cfg,
		es:              es,
		ss:              ss,
	}
	for _, o := range opts {
		o(aggr)
	}
	//	var zero T
	setAggregate(aggr)
	return aggr
}

// Executer is an interface that defines the Execute method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Decider interface.
type Executer[T any] interface {
	Execute(ctx context.Context, id ID[T], command Dispatcher[T], idempotencyKey string) ([]*Event[T], error)
	ExecuteUnique(ctx context.Context, id ID[T], command Dispatcher[T]) ([]*Event[T], error)
}

// Projector is an interface that defines the ProjectEvent method for projecting events on an aggregate.
// Events must implement the Event interface.
type Projector[T any] interface {
	Project(ctx context.Context, h allEventsHandler[T], opts ...ProjOption[T]) (Drainer, error)
}

// All aggregates must implement the Root interface.
type Root[T any] interface {
	Projector[T]
	Executer[T]
	NewID() ID[T]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber[T any] interface {
	Subscribe(ctx context.Context, handler func(envel *Event[T]) error, params *SubscribeParams[T]) (Drainer, error)
}

type ProjOption[T any] func(p *SubscribeParams[T])

type EventStream[T any] interface {
	Save(ctx context.Context, aggrID ID[T], msgs []*Event[T]) error
	Load(ctx context.Context, aggrID ID[T], fromSeq uint64) ([]*Event[T], error)
	subscriber[T]
}

type SnapshotStore[T any] interface {
	Save(ctx context.Context, aggrID ID[T], snap *Snapshot[T]) error
	Load(ctx context.Context, aggrID ID[T]) (*Snapshot[T], error)
}

// Aggregate root type it implements the Aggregate interface.
type root[T any] struct {
	AggregateConfig
	es     EventStream[T]
	ss     SnapshotStore[T]
	qos    qos.QoS
	pubsub *nats.Conn
}

type Snapshot[T any] struct {
	Timestamp time.Time
	State     *State[T]
}

type State[T any] struct {
	needSnapshot bool
	Version      uint64
	Timestamp    time.Time
	Body         *T
}

func (a *root[T]) build(ctx context.Context, id ID[T]) (*State[T], error) {
	state := &State[T]{Body: new(T)}

	sn, err := a.ss.Load(ctx, id)
	if err != nil {
		if !errors.Is(err, store.ErrNoSnapshot) {
			return nil, fmt.Errorf("build: %w", err)
		}
	}
	var snTimestamp time.Time
	if sn != nil {
		state = sn.State
		snTimestamp = sn.Timestamp
	}

	events, err := a.es.Load(ctx, id, state.Version)
	if err != nil {
		if errors.Is(err, store.ErrNoAggregate) {
			if sn == nil {
				return nil, nil
			}
		} else {
			return nil, fmt.Errorf("buid %w", err)
		}
	}

	for _, e := range events {
		e.Body.Evolve(state.Body)
	}
	if events != nil {
		lastEventIdx := len(events) - 1
		state.Version = events[lastEventIdx].Version
		state.Timestamp = events[lastEventIdx].Timestamp

		if messageCount(len(events)) >= messageCount(a.SnapthotMsgThreshold) && time.Since(snTimestamp) > a.SnapshotMaxInterval {
			state.needSnapshot = true
		}
	}

	return state, nil
}

func (a *root[T]) ExecuteUnique(ctx context.Context, id ID[T], command Dispatcher[T]) ([]*Event[T], error) {
	return a.Execute(ctx, id, command, id.String())
}

// Update executes a command on the aggregate root.
func (a *root[T]) Execute(
	ctx context.Context, id ID[T],
	command Dispatcher[T],
	idempKey string,
) ([]*Event[T], error) {
	if idempKey == "" {
		idempKey = a.NewID().String()
	}

	var err error

	state, err := a.build(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("build aggrigate: %w", err)
	}

	var evts Events[T]
	var version uint64
	if state != nil {
		evts, err = command.Dispatch(state.Body)
		version = state.Version
	} else {
		evts, err = command.Dispatch(new(T))
	}

	if err != nil {
		return nil, &InvariantViolationError{Err: err}
	}
	if evts == nil {
		return nil, nil
	}
	msgs := make([]*Event[T], len(evts))
	for i, ev := range evts {

		// Panics if event isn't registered
		kind := typereg.GetKind(ev)

		msgs[i] = &Event[T]{
			ID:          EventID[T](uuid.NewSHA1(id.UUID(), fmt.Appendf([]byte(idempKey), "%d", i))),
			PrevVersion: version,
			Kind:        kind,
			Body:        ev,
		}
	}

	if err := a.es.Save(ctx, id, msgs); err != nil {
		return nil, fmt.Errorf("update save: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	if state != nil && state.needSnapshot {

		go func() {

			err = a.ss.Save(ctx, id, &Snapshot[T]{Timestamp: time.Now(), State: state})
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "version", state.Version, "aggregateID", id.String(), "aggregate", reflect.TypeFor[T]().Name())

		}()

	}

	return msgs, nil
}
