package aggregate

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

func (*Root[T]) NewID() ID[T] {
	a, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return ID[T](a.String())
}

type ID[T any] string

func (i ID[T]) String() string {
	return string(i)
}

func (i ID[T]) UUID() uuid.UUID {
	u, err := uuid.Parse(string(i))
	if err != nil {
		panic(err)
	}
	return u
}

// SetTypeEncoder sets the default encoder for the aggregate,
// encoder must be a Serder implementation.
// Default is JSON.
func SetTypeEncoder(s Serder) {
	serde.SetDefaultSerder(s)
}

// NewUniqueIdempKeyForEvent creates a new idempotency key for the given aggregate ID and key.
// ID guarantees uniqueness across all events of the same aggregate.
func NewUniqueCommandIdempKey[C Command[T], T any](aggrID ID[T]) string {
	var ev C
	i, err := uuid.Parse(aggrID.String())

	if err != nil {
		panic(err)
	}
	return uuid.NewSHA1(i, []byte(typereg.TypeNameFrom(ev))).String()
}

type snapshotThreshold struct {
	numMsgs  byte
	interval time.Duration
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[T any](ctx context.Context, es eventStream[T], ss snapshotStore[T], opts ...Option[T]) *Root[T] {

	aggr := &Root[T]{
		snapshotThreshold: snapshotThreshold{interval: snapshotInterval, numMsgs: byte(snapshotSize)},
		es:                es,
		ss:                ss,
	}
	for _, o := range opts {
		o(aggr)
	}

	return aggr
}

type Envelope struct {
	ID            uuid.UUID
	Version       uint64
	AggregateID   string
	AggregateKind string
	Kind          string
	Payload       []byte
}

// Executer is an interface that defines the Execute method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Command interface.
type Executer[T any] interface {
	Execute(ctx context.Context, idempotencyKey string, command Command[T]) (CommandID[T], error)
}

// Projector is an interface that defines the Project method for projecting events on an aggregate.
// Events must implement the Event interface.
type Projector[T any] interface {
	Project(ctx context.Context, h EventHandler[T], opts ...ProjOption) (Drainer, error)
}

// All aggregates must implement the Aggregate interface.
type Aggregate[T any] interface {
	Projector[T]
	Executer[T]
	NewID() ID[T]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber interface {
	Subscribe(ctx context.Context, handler func(envel *Envelope) error, params *SubscribeParams) (Drainer, error)
}

type ProjOption func(p *SubscribeParams)

type eventStream[T any] interface {
	Save(ctx context.Context, aggrID string, idempotencyKey string, event *Envelope) error
	Load(ctx context.Context, aggrID string, fromSeq uint64) ([]*Envelope, error)
	subscriber
}

type snapshotStore[T any] interface {
	Save(ctx context.Context, aggrID string, snap []byte) error
	Load(ctx context.Context, aggrID string) ([]byte, error)
}

// Aggregate root type it implements the Aggregate interface.
type Root[T any] struct {
	snapshotThreshold snapshotThreshold
	es                eventStream[T]
	ss                snapshotStore[T]
	qos               qos.QoS
	pubsub            *nats.Conn
}

type snapshot[T any] struct {
	old       bool
	Timestamp time.Time
	MsgCount  messageCount
	Version   uint64
	Body      *T
}

func (a *Root[T]) build(ctx context.Context, id ID[T]) (*snapshot[T], error) {

	var snap snapshot[T]

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

// Execute executes a command on the aggregate root.
func (a *Root[T]) Execute(ctx context.Context, idempKey string, command Command[T]) (CommandID[T], error) {

	commandUUID := CommandID[T](uuid.NewSHA1(command.AggregateID().UUID(), []byte(idempKey)).String())
	var err error

	snap, err := a.build(ctx, command.AggregateID())
	if err != nil {
		return commandUUID, fmt.Errorf("build aggrigate: %w", err)
	}

	evt := command.Execute(snap.Body)

	if everr, ok := evt.(*EventError[T]); ok {
		slog.Warn("command execution error", "error", everr.Reason, "aggregate_id", command.AggregateID())
		return commandUUID, nil
	}

	b, err := serde.Serialize(evt)
	if err != nil {
		slog.Error("command serialize", "error", err)
		panic(err)
	}

	// Panics if event isn't registered
	kind := typereg.GuardType(evt)

	if err := a.es.Save(ctx, command.AggregateID().String(), commandUUID.String(), &Envelope{Version: snap.Version, Payload: b, Kind: kind}); err != nil {
		return commandUUID, fmt.Errorf("command: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	if snap.old {
		go func() {
			snap.Timestamp = time.Now()
			b, err := serde.Serialize(snap)
			if err != nil {
				slog.Warn("snapshot save serialization", "error", err.Error())
				return
			}
			err = a.ss.Save(ctx, command.AggregateID().String(), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "version", snap.Version, "aggregateID", command.AggregateID(), "msg_count", snap.MsgCount, "aggregate", reflect.TypeFor[T]().Name())

		}()

	}

	return commandUUID, nil
}
