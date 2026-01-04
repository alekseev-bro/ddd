package eventstore

// import (
// 	"context"
// 	"fmt"
// 	"log/slog"

// 	"github.com/alekseev-bro/ddd/internal/typereg"

// 	"github.com/alekseev-bro/ddd/pkg/qos"
// )

// type sagaHandlerFunc[E Event[T], C Command[U], T Aggregatable, U Aggregatable] func(event E) C

// type sagaHandler[E Event[T], C Command[U], T Aggregatable, U Aggregatable] struct {
// 	sub     EventProjector[T]
// 	cmd     Updater[U]
// 	handler sagaHandlerFunc[E, C, T, U]
// 	ot      qos.Ordering
// }

// func (sf *sagaHandler[E, C, T, U]) Handle(ctx context.Context, eventID EventID[T], event Event[T]) error {

// 	_, h := sf.cmd.Update(ctx, eventID.String(), sf.handler(event.(E)))
// 	return h

// }

// type sagaOption[E Event[T], C Command[U], T Aggregatable, U Aggregatable] func(*sagaHandler[E, C, T, U])

// func WithOrdering[E Event[T], C Command[U], T Aggregatable, U Aggregatable](ot qos.Ordering) sagaOption[E, C, T, U] {
// 	return func(sh *sagaHandler[E, C, T, U]) {
// 		sh.ot = ot
// 	}
// }

// func SagaStep[E Event[T], C Command[U], T Aggregatable, U Aggregatable](ctx context.Context, sub EventProjector[T], cmd Updater[U], shf sagaHandlerFunc[E, C, T, U], opts ...sagaOption[E, C, T, U]) {

// 	sh := &sagaHandler[E, C, T, U]{
// 		sub:     sub,
// 		cmd:     cmd,
// 		handler: shf,
// 	}

// 	for _, opt := range opts {
// 		opt(sh)
// 	}
// 	var (
// 		ee E
// 		cc C
// 		uu U
// 		tt T
// 	)
// 	ename := typereg.TypeNameFrom(ee)
// 	cname := typereg.TypeNameFrom(cc)
// 	sname := typereg.TypeNameFrom(tt)
// 	cmname := typereg.TypeNameFrom(uu)
// 	durname := fmt.Sprintf("%s:%s|%s:%s", sname, ename, cmname, cname)

// 	order := func() ProjOption {
// 		if sh.ot == qos.Ordered {
// 			return nil
// 		}
// 		return WithQoS(qos.QoS{Ordering: qos.Unordered})
// 	}
// 	d, err := sub.ProjectEvent(ctx, sh, WithName(durname), order(), WithFilterByEvent[E]())
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
