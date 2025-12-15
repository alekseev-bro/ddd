package aggregate

// All commands must implement the Command interface.
type Command[T any] interface {
	Execute(entity *T) Event[T]
	identer[T]
}

type identer[T any] interface {
	AggregateID() ID[T]
}

// Is an alias for ID[Command[T]]
type CommandID[T any] = ID[Command[T]]
