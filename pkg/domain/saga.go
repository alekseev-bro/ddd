package domain

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/qos"
)

type sagaHandlerFunc[E aggregate.Event[T], C aggregate.Command[U], T any, U any] func(event E) C

type sagaHandler[E aggregate.Event[T], C aggregate.Command[U], T any, U any] struct {
	sub     aggregate.Projector[T]
	cmd     aggregate.Executer[U]
	handler sagaHandlerFunc[E, C, T, U]
	ot      qos.Ordering
}

func (sf *sagaHandler[E, C, T, U]) Handle(ctx context.Context, eventID aggregate.EventID[T], event aggregate.Event[T]) error {

	_, h := sf.cmd.Execute(ctx, eventID.String(), sf.handler(event.(E)))
	return h

}

type sagaOption[E aggregate.Event[T], C aggregate.Command[U], T any, U any] func(*sagaHandler[E, C, T, U])

func WithOrdering[E aggregate.Event[T], C aggregate.Command[U], T any, U any](ot qos.Ordering) sagaOption[E, C, T, U] {
	return func(sh *sagaHandler[E, C, T, U]) {
		sh.ot = ot
	}
}

func SagaStep[E aggregate.Event[T], C aggregate.Command[U], T any, U any](ctx context.Context, sub aggregate.Projector[T], cmd aggregate.Executer[U], shf sagaHandlerFunc[E, C, T, U], opts ...sagaOption[E, C, T, U]) {

	sh := &sagaHandler[E, C, T, U]{
		sub:     sub,
		cmd:     cmd,
		handler: shf,
	}

	for _, opt := range opts {
		opt(sh)
	}
	var (
		ee E
		cc C
		uu U
		tt T
	)
	ename := typereg.TypeNameFrom(ee)
	cname := typereg.TypeNameFrom(cc)
	sname := typereg.TypeNameFrom(tt)
	cmname := typereg.TypeNameFrom(uu)
	durname := fmt.Sprintf("%s:%s|%s:%s", sname, ename, cmname, cname)

	order := func() aggregate.ProjOption {
		if sh.ot == qos.Ordered {
			return nil
		}
		return aggregate.WithQoS(qos.QoS{Ordering: qos.Unordered})
	}
	d, err := sub.Project(ctx, sh, aggregate.WithName(durname), order(), aggregate.WithFilterByEvent[E]())
	if err != nil {
		slog.Error("failed to project saga handler", "error", err)
		panic(err)
	}

	go func() {
		<-ctx.Done()
		if err := d.Drain(); err != nil {
			slog.Error("failed to drain saga handler", "error", err, "saga", durname)
		}
	}()

}
