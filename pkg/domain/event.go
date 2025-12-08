package domain

type EventID[T any] = ID[Event[T]]

// EventError is not saved to the event store.
type EventError[T any] struct {
	AggID  ID[T]
	Reason string
}

func (e EventError[T]) Error() string {
	return e.Reason
}

func (e *EventError[T]) Apply(*T) {}

type Event[T any] interface {
	Apply(*T)
}

func RegisterEvent[E Event[T], T any](reg registry) {
	var ev E
	reg.register(ev)
}
