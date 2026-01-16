package aggregate

import "context"

type Evolver[T any] interface {
	Evolve(*T)
}

func NewEvents[T any](events ...Evolver[T]) Events[T] {
	return events
}

type Events[T any] []Evolver[T]

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

type Event[T any] struct {
	ID      ID
	Kind    string
	Version uint64
	Body    Evolver[T]
}

func (e *Event[T]) Evolve(aggr *T) {
	e.Body.Evolve(aggr)
}
