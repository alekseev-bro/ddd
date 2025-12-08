package domain

import (
	"context"
	"fmt"
	"log/slog"

	reg "github.com/alekseev-bro/ddd/internal/registry"
)

type sagaHandlerFunc[E Event[T], C Command[U], T any, U any] func(event E) C

type sagaHandler[E Event[T], C Command[U], T any, U any] struct {
	sub     projector[T]
	cmd     executer[U]
	handler sagaHandlerFunc[E, C, T, U]
}

func (sf *sagaHandler[E, C, T, U]) Handle(ctx context.Context, eventID EventID[T], event Event[T]) error {

	return sf.cmd.Execute(ctx, eventID.String(), sf.handler(event.(E)))

}

func Saga[E Event[T], C Command[U], T any, U any](ctx context.Context, sub projector[T], cmd executer[U], shf sagaHandlerFunc[E, C, T, U]) Drainer {

	sh := &sagaHandler[E, C, T, U]{
		sub:     sub,
		cmd:     cmd,
		handler: shf,
	}
	var (
		ee E
		cc C
		uu U
		tt T
	)

	ename := reg.TypeNameFrom(ee)
	cname := reg.TypeNameFrom(cc)
	sname := reg.TypeNameFrom(tt)
	cmname := reg.TypeNameFrom(uu)
	durname := fmt.Sprintf("%s:%s|%s:%s", sname, ename, cmname, cname)

	d, err := sub.Project(ctx, sh, WithName(durname), WithUnordered(), FilterByEvent[E]())
	if err != nil {
		slog.Error("failed to project saga handler", "error", err)
		panic(err)
	}

	return d[0]

}
