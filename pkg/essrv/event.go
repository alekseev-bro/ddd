package essrv

import "time"

type EventID[T any] = ID[Evolver[T]]

type Evolver[T any] interface {
	Evolve(*T)
}

func NewEvents[T any](events ...Evolver[T]) Events[T] {
	return events
}

type Events[T any] []Evolver[T]

type Event[T any] struct {
	ID          EventID[T]
	AggregateID ID[T]
	PrevVersion uint64
	Version     uint64
	Kind        string
	Timestamp   time.Time
	Body        Evolver[T]
}

func (e *Event[T]) Evolve(aggr *T) {

	e.Body.Evolve(aggr)
}

type Created[T any] struct {
	State *T
}

func (e Created[T]) Evolve(state *T) {
	*state = *e.State
}

func NewCreatedEvent[T any](state *T) Created[T] {
	return Created[T]{State: state}
}
