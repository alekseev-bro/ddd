package essrv

import (
	"context"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/typereg"

	"github.com/alekseev-bro/ddd/pkg/qos"
)

type SubscribeParams[T any] struct {
	DurableName string
	Kind        []string
	AggrID      string
	QoS         qos.QoS
}

func WithFilterByAggregateID[T any](id ID[T]) ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		p.AggrID = id.String()
	}
}

func WithFilterByEvent[E Evolver[T], T any]() ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		var ev E
		p.Kind = append(p.Kind, typereg.TypeNameFrom(ev))
	}
}

func WithName[T any](name string) ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		p.DurableName = name
	}
}

func WithQoS[T any](qos qos.QoS) ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		p.QoS = qos
	}
}

type allEventsHandler[T any] interface {
	HandleAllEvents(ctx context.Context, event *Event[T]) error
}

func (a *root[T]) Project(ctx context.Context, h allEventsHandler[T], opts ...ProjOption[T]) (Drainer, error) {
	dn := typereg.TypeNameFrom(h)
	params := &SubscribeParams[T]{
		DurableName: dn,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(params)
		}
	}
	d, err := a.es.Subscribe(ctx, func(event *Event[T]) error {

		return h.HandleAllEvents(ctx, event)
	}, params)
	if err == nil {
		slog.Info("subscription created", "subscription", params.DurableName)
	}
	return d, nil
}
