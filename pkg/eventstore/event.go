package eventstore

type EventID[T any] = ID[Event[T]]

type Event[T any] interface {
	Apply(*T)
}

func NewEvents[T any](events ...Event[T]) Events[T] {
	return events
}

type Events[T any] []Event[T]
