package events

import "time"

type EventID[T any] = ID[Evolver[T]]

type Evolver[T any] interface {
	Evolve(*T)
}

func New[T any](events ...Evolver[T]) Events[T] {
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
