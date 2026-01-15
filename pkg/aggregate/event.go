package aggregate

import "context"

type Evolver[T Root] interface {
	Evolve(*T)
}

func NewEvents[T Root](events ...Evolver[T]) Events[T] {
	return events
}

type Events[T Root] []Evolver[T]

func (e Events[T]) Evolve(aggr *T) {
	for _, ev := range e {
		ev.Evolve(aggr)
	}
}

var idempKeyCtx idempotanceKey = "idempKey"

type idempotanceKey string

func idempotanceKeyFromCtxOrRandom(ctx context.Context) string {
	if val, ok := ctx.Value(idempKeyCtx).(string); ok {
		return val
	}
	return NewID().String()
}

func ContextWithIdempotancyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idempKeyCtx, key)
}

type Event[T Root] struct {
	ID               ID
	AggregateID      ID
	ExpectedSequence uint64
	Kind             string
	Version
	Body Evolver[T]
}

func (e *Event[T]) Evolve(aggr *T) {

	e.Body.Evolve(aggr)
}
