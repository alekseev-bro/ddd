package essrv

type Dispatcher[T any] interface {
	Dispatch(aggr *T) (Events[T], error)
}

type Create[T any] struct {
	State  *T
	Events []Evolver[T]
}

func (cmd Create[T]) Dispatch(state *T) (Events[T], error) {
	if state == nil {
		return NewEvents(Created[T]{State: cmd.State}), nil
	}
	return nil, nil
}

func NewCreateCMD[T any](state *T, events ...Evolver[T]) Create[T] {
	return Create[T]{State: state, Events: events}
}
