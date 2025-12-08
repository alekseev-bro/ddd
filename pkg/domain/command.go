package domain

type Command[T any] interface {
	Execute(entity *T) Event[T]
	identer[T]
}

type identer[T any] interface {
	AggregateID() ID[T]
}
