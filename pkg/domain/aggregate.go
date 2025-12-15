package domain

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
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

type ID[T any] string

type Serder serde.Serder

func (*aggregate[T]) NewID() ID[T] {
	a, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return ID[T](a.String())
}

// Default is JSON
func SetTypeEncoder(s Serder) {
	serde.SetDefaultSerder(s)
}

func NewIdempotencyKey[T any](id ID[T], key string) string {
	i, err := uuid.Parse(string(id))
	if err != nil {
		panic(err)
	}

	return uuid.NewSHA1(i, []byte(key)).String()
}

type option[T any] func(a *aggregate[T])

type snapshotThreshold struct {
	numMsgs  byte
	interval time.Duration
}

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T any](numMsgs byte, interval time.Duration) option[T] {
	return func(a *aggregate[T]) {
		a.snapshotThreshold.numMsgs = numMsgs
		a.snapshotThreshold.interval = interval
	}
}

func NewAggregate[T any](ctx context.Context, es eventStream[T], ss snapshotStore[T], opts ...option[T]) *aggregate[T] {

	aggr := &aggregate[T]{
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
	ID      uuid.UUID
	Version uint64
	Kind    string
	Payload []byte
}

type Executer[T any] interface {
	Execute(ctx context.Context, idempotencyKey string, command Command[T], opts ...commandOption) (CommandID[T], error)
}
type Projector[T any] interface {
	Project(ctx context.Context, h EventHandler[T], opts ...ProjOption) (Drainer, error)
}

type Aggregate[T any] interface {
	Projector[T]
	Executer[T]
	NewID() ID[T]
}

type registry interface {
	register(any)
}

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

type aggregate[T any] struct {
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

func (a *aggregate[T]) register(t any) {
	typereg.Register(t)
}

func (a *aggregate[T]) build(ctx context.Context, id ID[T]) (*snapshot[T], error) {

	var snap snapshot[T]

	rec, err := a.ss.Load(ctx, string(id))
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

	envelopes, err := a.es.Load(ctx, string(id), snap.Version)
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

// type CommandFunc[T any] func(*T) Event[T]

// func (f CommandFunc[T]) Execute(t *T) Event[T] {
// 	return f(t)
// }

// func (a *aggregate[T]) CommandFunc(ctx context.Context, idempKey string, command func(*T) Event[T]) error {
// 	return a.Execute(ctx, idempKey, CommandFunc[T](command))
// }

type commandOptions struct {
	waitTimeout  time.Duration
	waitProjSync bool
}

func (o *commandOptions) WithWaitProjSync(waitTimeout time.Duration) *commandOptions {
	o.waitProjSync = true
	o.waitTimeout = waitTimeout
	return o
}

type commandOption func(*commandOptions)

func (a *aggregate[T]) Execute(ctx context.Context, idempKey string, command Command[T], opts ...commandOption) (CommandID[T], error) {
	o := &commandOptions{
		waitTimeout:  time.Second,
		waitProjSync: false,
	}
	for _, opt := range opts {
		opt(o)
	}
	commandUUID := CommandID[T](NewIdempotencyKey(command.AggregateID(), idempKey))
	var err error

	snap, err := a.build(ctx, command.AggregateID())
	if err != nil {
		return commandUUID, fmt.Errorf("build aggrigate: %w", err)
	}

	evt := command.Execute(snap.Body)

	if everr, ok := evt.(*EventError[T]); ok {
		slog.Warn("command execution error", "error", everr.Reason, "aggregate_id", string(command.AggregateID()))
		return commandUUID, nil
	}

	b, err := serde.Serialize(evt)
	if err != nil {
		slog.Error("command serialize", "error", err)
		panic(err)
	}

	// Panics if event isn't registered
	kind := typereg.GuardType(evt)

	if err := a.es.Save(ctx, string(command.AggregateID()), string(commandUUID), &Envelope{Version: snap.Version, Payload: b, Kind: kind}); err != nil {
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
			err = a.ss.Save(ctx, string(command.AggregateID()), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "version", snap.Version, "aggregateID", string(command.AggregateID()), "msg_count", snap.MsgCount, "aggregate", reflect.TypeFor[T]().Name())

		}()

	}

	return commandUUID, nil
}
