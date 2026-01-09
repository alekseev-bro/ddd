package events

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

func ProjectEvent[E Evolver[T], T any](ctx context.Context, h oneEventsHandler[E, T]) (Drainer, error) {
	proj := NewStore[T](ctx, nil, nil, AggregateConfig{})
	var zero E
	return proj.Project(ctx, &handleOneAdapter[E, T]{h: h}, WithFilterByEvent[E](), WithName[T](typereg.TypeNameFrom(zero)))

}

type handleOneAdapter[E Evolver[T], T any] struct {
	h oneEventsHandler[E, T]
}

func (h *handleOneAdapter[E, T]) Handle(ctx context.Context, eventID string, event Evolver[T]) error {
	return h.h.Handle(ctx, eventID, event.(E))
}

type oneEventsHandler[E Evolver[T], T any] interface {
	Handle(ctx context.Context, eventID string, event E) error
}

type allEventsHandler[T any] interface {
	Handle(ctx context.Context, eventID string, event Evolver[T]) error
}

func (a *store[T]) Project(ctx context.Context, h allEventsHandler[T], opts ...ProjOption[T]) (Drainer, error) {
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

		return h.Handle(ctx, event.ID.String(), event.Body)
	}, params)
	if err == nil {
		slog.Info("subscription created", "subscription", params.DurableName)
	}
	return d, nil
}
