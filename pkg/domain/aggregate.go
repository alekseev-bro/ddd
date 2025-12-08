package domain

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"fmt"

	"github.com/alekseev-bro/ddd/pkg/store"

	"github.com/alekseev-bro/ddd/internal/serde"

	reg "github.com/alekseev-bro/ddd/internal/registry"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// var nc *nats.Conn

type messageCount uint

const (
	snapshotSize messageCount = 100
)

type ID[T any] string

func (i ID[T]) String() string {

	return string(i)
}

func NewID[T any]() ID[T] {
	a, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return ID[T](a.String())
}

func NewIdempotencyKey[T any](id ID[T], key string) string {
	i, err := uuid.Parse(string(id))
	if err != nil {
		panic(err)
	}
	return uuid.NewMD5(i, []byte(key)).String()
}

type option[T any] func(a *aggregate[T])

func WithSerde[T any](s serde.Serder) option[T] {
	return func(a *aggregate[T]) {
		a.tr = reg.New(s)
	}
}

func NewAggregate[T any](ctx context.Context, es eventStream[T], ss snapshotStore[T], opts ...option[T]) *aggregate[T] {

	//ent := PT(new(T))
	// for _, v := range ent.RegisterEvents() {
	// 	etype := reflect.TypeOf(v)
	// 	if etype.Kind() == reflect.Ptr {
	// 		panic("RegisterEvents return type must be a slice of values")
	// 	}

	// 	eventDefaultRegistry[fmt.Sprintf("%s_%s", aname, etype.Name())] = v
	// 	eventNamesRegistry[v] = etype.Name()

	// }

	aggr := &aggregate[T]{
		es: es,
		ss: ss,
		tr: reg.New(serde.NewDefaultSerder()),

		//serder:          &DefaultSerder[T]{},
	}
	for _, o := range opts {
		o(aggr)
	}

	//var ent T
	// ent.Events(func(e Applyable[T]) {

	// 	store.eventRegistry.Add(e)
	// })
	// for _, v := range ent.Events() {

	// }
	//	st, ok := streams[ag.Domain().Type]
	//if !ok {

	//	streams[ag.Domain().Type] = st
	//}

	return aggr
}

type Envelope struct {
	ID      uuid.UUID
	Version uint64
	Kind    string
	Payload []byte
}

// type typeStore interface {
// 	serde.Serder
// 	typeGuardGetter
// 	registry
// }

type executer[T any] interface {
	Execute(ctx context.Context, idempotencyKey string, command Command[T], opts ...commandOption) error
	// CommandFunc(ctx context.Context, command func(*T) Event[T]) error
}
type projector[T any] interface {
	Project(ctx context.Context, h EventHandler[T], opts ...ProjOption) ([]Drainer, error)
}

type Aggregate[T any] interface {
	projector[T]
	executer[T]
}

type registry interface {
	register(any)
}

type Drainer interface {
	Drain() error
}

type subscriber[T any] interface {
	Subscribe(ctx context.Context, handler func(envel *Envelope) error, params *SubscribeParams) ([]Drainer, error)
}

type ProjOption func(p *SubscribeParams)

type eventStream[T any] interface {
	Save(ctx context.Context, aggrID string, idempotencyKey string, events []*Envelope) error
	Load(ctx context.Context, aggrID string, fromSeq uint64, handler func(event *Envelope)) (uint64, error)
	subscriber[T]
}

type snapshotStore[T any] interface {
	Save(ctx context.Context, aggrID string, snap []byte) error
	Load(ctx context.Context, aggrID string) ([]byte, error)
}

//	type Registry[T Reducible[T]] interface {
//		Register(Applyable[T])
//	}

type aggregate[T any] struct {
	tr     *reg.Type
	es     eventStream[T]
	ss     snapshotStore[T]
	pubsub *nats.Conn
}

type snapshot[T any] struct {
	MsgCount messageCount
	Version  uint64
	Body     *T
}

func (a *aggregate[T]) register(t any) {
	a.tr.Register(t)
}

func (a *aggregate[T]) build(ctx context.Context, id ID[T]) (*snapshot[T], error) {

	ent := new(T)
	//var snap Snapshot[T]
	// rec, err := a.snap.Get(ctx, id)
	// if err != nil {
	// 	if !errors.Is(err, store.ErrNoSnapshot) {
	// 		return nil, fmt.Errorf("build: %w", err)
	// 	}
	// 	if err := a.serder.Deserialize(rec, &snap); err != nil {
	// 		return nil, fmt.Errorf("build: %w", err)
	// 	}

	// } else {
	// 	if err := a.serder.Deserialize(rec, &snap); err != nil {
	// 		return nil, fmt.Errorf("build: %w", err)
	// 	}
	// }

	var totalMsgs messageCount

	last, err := a.es.Load(ctx, id.String(), 0, func(e *Envelope) {

		ev := a.tr.GetType(e.Kind, e.Payload)

		ev.(Event[T]).Apply(ent)
		totalMsgs++

	})
	if err != nil {
		return nil, fmt.Errorf("buid %w", err)
	}
	sn := &snapshot[T]{Version: last, Body: ent, MsgCount: totalMsgs}

	return sn, nil
}

// type CommandFunc[T any] func(*T) Event[T]

// func (f CommandFunc[T]) Execute(t *T) Event[T] {
// 	return f(t)
// }

// func (a *aggregate[T]) CommandFunc(ctx context.Context, command func(*T) Event[T]) error {
// 	return a.Command(ctx, CommandFunc[T](command))
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

func (a *aggregate[T]) Execute(ctx context.Context, idempKey string, command Command[T], opts ...commandOption) error {
	o := &commandOptions{
		waitTimeout:  time.Second,
		waitProjSync: false,
	}
	for _, opt := range opts {
		opt(o)
	}
	var err error

	snap := &snapshot[T]{}

	snap, err = a.build(ctx, command.AggregateID())
	if err != nil {
		if !errors.Is(err, store.ErrNoAggregate) {
			return fmt.Errorf("build aggrigate: %w", err)
		}
		snap = &snapshot[T]{}
	}

	evt := command.Execute(snap.Body)

	if _, ok := evt.(*EventError[T]); ok {
		return nil
	}

	b, err := a.tr.Serder.Serialize(evt)
	if err != nil {
		slog.Error("command serialize", "error", err)
		panic(err)
	}

	// Panics if event isn't registered
	kind := a.tr.GuardType(evt)

	eventUUID := NewIdempotencyKey(command.AggregateID(), idempKey)

	if err := a.es.Save(ctx, command.AggregateID().String(), eventUUID, []*Envelope{{Version: snap.Version, Payload: b, Kind: kind}}); err != nil {
		return fmt.Errorf("command: %w", err)
	}
	// Save snapshot if aggregate has more than snapshotSize messages
	// if snap != nil {
	// 	if snap.MsgCount >= snapshotSize {
	// 		go func() {
	// 			b, err := a.serder.Serialize(snap)
	// 			if err != nil {
	// 				slog.Warn(err.Error())
	// 			}
	// 			a.ss.Save(ctx, command.AggregateID(), b)
	// 		}()

	// 	}
	// }

	return nil
}
