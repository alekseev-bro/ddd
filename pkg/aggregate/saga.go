package aggregate

// type sagaHandlerFunc[Event Evolver[T], Command Dispatcher[U], T any, U any] func(event Event) (Command, ID[U])

// type sagaHandler[Event Evolver[T], Command Dispatcher[U], T any, U any] struct {
// 	sub     Projector[T]
// 	exe     Executer[U]
// 	handler sagaHandlerFunc[Event, Command, T, U]
// 	ot      qos.Ordering
// }

// func (sf *sagaHandler[E, C, T, U]) HandleAllEvents(ctx context.Context, event *Event[T]) error {
// 	cmd, id := sf.handler(event.Body.(E))
// 	_, err := sf.exe.Execute(ctx, id, cmd, event.ID.String())
// 	return err
// }

// type sagaOption[Event Evolver[T], Command Dispatcher[U], T any, U any] func(*sagaHandler[Event, Command, T, U])

// func WithOrdering[E Evolver[T], C Dispatcher[U], T any, U any](ot qos.Ordering) sagaOption[E, C, T, U] {
// 	return func(sh *sagaHandler[E, C, T, U]) {
// 		sh.ot = ot
// 	}
// }

// func SagaStep[Event Evolver[T], Command Dispatcher[U], T any, U any](ctx context.Context, shf func(event Event) (Command, ID[U]), opts ...sagaOption[Event, Command, T, U]) {
// 	if t, ok := getAggregate[T](); ok {
// 		if u, ok := getAggregate[U](); ok {
// 			sagaStep(ctx, t, u, shf, opts...)
// 			return
// 		}
// 	}
// 	panic("aggregate not found")

// }

// func sagaStep[Event Evolver[T], Command Dispatcher[U], T any, U any](ctx context.Context, sub Projector[T], exe Executer[U], shf func(event Event) (Command, ID[U]), opts ...sagaOption[Event, Command, T, U]) {

// 	sh := &sagaHandler[Event, Command, T, U]{
// 		sub:     sub,
// 		exe:     exe,
// 		handler: shf,
// 	}

// 	for _, opt := range opts {
// 		opt(sh)
// 	}
// 	var (
// 		ee Event
// 		cc Command
// 		uu U
// 		tt T
// 	)
// 	ename := typereg.TypeNameFrom(ee)
// 	cname := typereg.TypeNameFrom(cc)
// 	sname := typereg.TypeNameFrom(tt)
// 	cmname := typereg.TypeNameFrom(uu)
// 	durname := fmt.Sprintf("%s:%s|%s:%s", sname, ename, cmname, cname)

// 	order := func() ProjOption[T] {
// 		if sh.ot == qos.Ordered {
// 			return nil
// 		}
// 		return WithQoS[T](qos.QoS{Ordering: qos.Unordered})
// 	}
// 	d, err := sub.Project(ctx, sh, WithName[T](durname), order(), WithFilterByEvent[Event]())
// 	if err != nil {
// 		slog.Error("failed to project saga handler", "error", err)
// 		panic(err)
// 	}

// 	go func() {
// 		<-ctx.Done()
// 		if err := d.Drain(); err != nil {
// 			slog.Error("failed to drain saga handler", "error", err, "saga", durname)
// 		}
// 	}()

// }
