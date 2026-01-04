package eventstore

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
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

func (*eventStore[T]) NewID() ID[T] {
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

type snapshotThreshold struct {
	numMsgs  byte
	interval time.Duration
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[T any](ctx context.Context, es EventStream[T], ss SnapshotStore[T], opts ...Option[T]) *eventStore[T] {

	aggr := &eventStore[T]{
		snapshotThreshold: snapshotThreshold{interval: snapshotInterval, numMsgs: byte(snapshotSize)},
		es:                es,
		ss:                ss,
	}
	for _, o := range opts {
		o(aggr)
	}
	//	var zero T

	return aggr
}

type Message struct {
	ID      uuid.UUID
	Version uint64
	Kind    string
	Payload []byte
}

// Updater is an interface that defines the Execute method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Command interface.
type Updater[T any] interface {
	Update(ctx context.Context, id ID[T], idempotencyKey string, modify command[T]) (EventID[T], error)
}

type Creator[T any] interface {
	Create(ctx context.Context, id ID[T], init command[T]) (ID[T], error)
}

// EventProjector is an interface that defines the ProjectEvent method for projecting events on an aggregate.
// Events must implement the Event interface.
type EventProjector[T any] interface {
	ProjectEvent(ctx context.Context, h EventHandler[T], opts ...ProjOption) (Drainer, error)
}

// All aggregates must implement the EventStore interface.
type EventStore[T any] interface {
	EventProjector[T]
	Updater[T]
	Creator[T]
	NewID() ID[T]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber interface {
	Subscribe(ctx context.Context, handler func(envel *Message) error, params *SubscribeParams) (Drainer, error)
}

type ProjOption func(p *SubscribeParams)

type EventStream[T any] interface {
	Save(ctx context.Context, aggrID string, idempotencyKey string, msgs []*Message) error
	Load(ctx context.Context, aggrID string, fromSeq uint64) ([]*Message, error)
	subscriber
}

type SnapshotStore[T any] interface {
	Save(ctx context.Context, aggrID string, snap []byte) error
	Load(ctx context.Context, aggrID string) ([]byte, error)
}

// Aggregate root type it implements the Aggregate interface.
type eventStore[T any] struct {
	snapshotThreshold snapshotThreshold
	es                EventStream[T]
	ss                SnapshotStore[T]
	qos               qos.QoS
	pubsub            *nats.Conn
}

type aggregate[T any] struct {
	old       bool
	Timestamp time.Time
	MsgCount  messageCount
	Version   uint64
	Body      *T
}

func (a *eventStore[T]) build(ctx context.Context, id ID[T]) (*aggregate[T], error) {

	var snap aggregate[T]

	rec, err := a.ss.Load(ctx, id.String())
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNoSnapshot):
			snap.Body = new(T)
		default:
			return nil, fmt.Errorf("build: %w", err)
		}
	} else {
		if err := serde.Deserialize(rec, &snap); err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
	}

	envelopes, err := a.es.Load(ctx, id.String(), snap.Version)
	if err != nil {
		if !errors.Is(err, store.ErrNoAggregate) {
			return nil, fmt.Errorf("buid %w", err)
		}

	}
	if envelopes != nil {
		for _, e := range envelopes {
			ev := typereg.GetType(e.Kind, e.Payload)

			ev.(Event[T]).Apply(snap.Body)

		}

		snap.Version = envelopes[len(envelopes)-1].Version
		snap.MsgCount = snap.MsgCount + messageCount(len(envelopes))
		if messageCount(len(envelopes)) >= messageCount(a.snapshotThreshold.numMsgs) && time.Since(snap.Timestamp) > a.snapshotThreshold.interval {
			snap.old = true
		}
	}

	if snap.MsgCount == 0 {
		snap.Body = nil
	}

	return &snap, nil
}

type command[T any] func(*T) (Events[T], error)

func (a *eventStore[T]) Create(
	ctx context.Context, id ID[T],
	initCmd command[T],
) (ID[T], error) {

	evts, err := initCmd(new(T))
	if err != nil {
		return id, fmt.Errorf("init event: %w", err)
	}
	if evts == nil {
		return id, nil
	}
	msgs := make([]*Message, len(evts))
	for i, ev := range evts {
		b, err := serde.Serialize(ev)
		if err != nil {
			slog.Error("command serialize", "error", err)
			panic(err)
		}
		// Panics if event isn't registered
		kind := typereg.GuardType(ev)
		msgs[i] = &Message{
			Version: 0,
			Kind:    kind,
			Payload: b,
		}
	}

	err = a.es.Save(ctx, id.String(), id.String(), msgs)
	if err != nil {
		slog.Error("create save", "error", err.Error())
		return id, fmt.Errorf("create: %w", err)
	}

	return id, nil
}

// Update executes a command on the aggregate root.
func (a *eventStore[T]) Update(
	ctx context.Context, id ID[T],
	idempKey string,
	modify command[T],
) (EventID[T], error) {
	if idempKey == "" {
		idempKey = uuid.New().String()
	}
	eventID := EventID[T](uuid.NewSHA1(id.UUID(), []byte(idempKey)))

	var err error

	agg, err := a.build(ctx, id)
	if err != nil {
		return eventID, fmt.Errorf("build aggrigate: %w", err)
	}

	evts, err := modify(agg.Body)

	if err != nil {
		return eventID, &InvariantViolationError{Err: err}
	}
	if evts == nil {
		return eventID, nil
	}
	msgs := make([]*Message, len(evts))
	for i, ev := range evts {
		b, err := serde.Serialize(ev)
		if err != nil {
			slog.Error("update serialize", "error", err)
			panic(err)
		}
		// Panics if event isn't registered
		kind := typereg.GuardType(ev)
		msgs[i] = &Message{
			Version: agg.Version,
			Kind:    kind,
			Payload: b,
		}
	}

	if err := a.es.Save(ctx, id.String(), eventID.String(), msgs); err != nil {
		return eventID, fmt.Errorf("update save: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	if agg.old {

		go func() {
			agg.Timestamp = time.Now()
			b, err := serde.Serialize(agg)
			if err != nil {
				slog.Warn("snapshot save serialization", "error", err.Error())
				return
			}

			err = a.ss.Save(ctx, id.String(), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "version", agg.Version, "aggregateID", id.String(), "msg_count", agg.MsgCount, "aggregate", reflect.TypeFor[T]().Name())

		}()

	}

	return eventID, nil
}
