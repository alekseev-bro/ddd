package aggregate

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/typereg"

	"github.com/alekseev-bro/ddd/pkg/qos"
)

type SubscribeParams[T Root] struct {
	DurableName string
	Kind        []string
	AggrID      string
	QoS         qos.QoS
}

func WithFilterByAggregateID[T Root](id ID) ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		p.AggrID = id.String()
	}
}

func WithFilterByEvent[E Evolver[T], T Root]() ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		var ev E
		p.Kind = append(p.Kind, typereg.GetKind(ev))
	}
}

func WithName[T Root](name string) ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		p.DurableName = name
	}
}

func WithQoS[T Root](qos qos.QoS) ProjOption[T] {
	return func(p *SubscribeParams[T]) {
		p.QoS = qos
	}
}

func Project[E Evolver[T], T Root](ctx context.Context, sub Subscriber[T], h EventHandler[T, E]) (Drainer, error) {
	var zero E

	n := fmt.Sprintf("%s|%s", typereg.TypeNameFrom(h), typereg.GetKind(zero))

	return sub.Subscribe(ctx, &handleEventAdapter[E, T]{h: h}, WithFilterByEvent[E](), WithName[T](n))

}

type handleEventAdapter[E Evolver[T], T Root] struct {
	h EventHandler[T, E]
}

func (h *handleEventAdapter[E, T]) HandleEvents(ctx context.Context, event Evolver[T]) error {
	return h.h.HandleEvent(ctx, event.(E))
}

// Subscribe creates a new subscription on aggegate events with the given handler.
func (a *store[T, PT]) Subscribe(ctx context.Context, h EventsHandler[T], opts ...ProjOption[T]) (Drainer, error) {
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
		return h.HandleEvents(context.WithValue(ctx, idempKeyCtx, event.ID), event.Body)
	}, params)
	if err == nil {
		slog.Info("subscription created", "subscription", params.DurableName)
	}
	return d, nil
}
