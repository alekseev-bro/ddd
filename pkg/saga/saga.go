package saga

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/domain"
	"github.com/alekseev-bro/ddd/pkg/qos"
)

type sagaHandlerFunc[E domain.Event[T], C domain.Command[U], T any, U any] func(event E) C

type sagaHandler[E domain.Event[T], C domain.Command[U], T any, U any] struct {
	sub     domain.Projector[T]
	cmd     domain.Executer[U]
	handler sagaHandlerFunc[E, C, T, U]
	ot      qos.Ordering
}

func (sf *sagaHandler[E, C, T, U]) Handle(ctx context.Context, eventID domain.EventID[T], event domain.Event[T]) error {
	_, h := sf.cmd.Execute(ctx, string(eventID), sf.handler(event.(E)))
	return h

}

type sagaOption[E domain.Event[T], C domain.Command[U], T any, U any] func(*sagaHandler[E, C, T, U])

func WithOrdering[E domain.Event[T], C domain.Command[U], T any, U any](ot qos.Ordering) sagaOption[E, C, T, U] {
	return func(sh *sagaHandler[E, C, T, U]) {
		sh.ot = ot
	}
}

func Step[E domain.Event[T], C domain.Command[U], T any, U any](ctx context.Context, sub domain.Projector[T], cmd domain.Executer[U], shf sagaHandlerFunc[E, C, T, U], opts ...sagaOption[E, C, T, U]) {

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

	order := func() domain.ProjOption {
		if sh.ot == qos.Ordered {
			return nil
		}
		return domain.WithQoS(qos.QoS{Ordering: qos.Unordered})
	}
	d, err := sub.Project(ctx, sh, domain.WithName(durname), order(), domain.WithFilterByEvent[E]())
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
