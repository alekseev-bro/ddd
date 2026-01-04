package eventstore

import (
	"context"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/qos"
)

type SubscribeParams struct {
	DurableName string
	Kind        []string
	AggrID      string
	QoS         qos.QoS
}

func WIthFilterByAggregateID[T Aggregate[T]](id ID[T]) ProjOption {
	return func(p *SubscribeParams) {
		p.AggrID = id.String()
	}
}

func WithFilterByEvent[E Event[T], T any, PT Aggregate[T]]() ProjOption {
	return func(p *SubscribeParams) {
		var ev E
		p.Kind = append(p.Kind, typereg.TypeNameFrom(ev))
	}
}

func WithName(name string) ProjOption {
	return func(p *SubscribeParams) {
		p.DurableName = name
	}
}

func WithQoS(qos qos.QoS) ProjOption {
	return func(p *SubscribeParams) {
		p.QoS = qos
	}
}

type EventHandler[T any] interface {
	Handle(ctx context.Context, eventID EventID[T], event Event[T]) error
}

func (a *eventStore[T]) ProjectEvent(ctx context.Context, h EventHandler[T], opts ...ProjOption) (Drainer, error) {
	dn := typereg.TypeNameFrom(h)
	params := &SubscribeParams{
		DurableName: dn,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(params)
		}
	}
	d, err := a.es.Subscribe(ctx, func(envel *Message) error {
		ev := typereg.GetType(envel.Kind, envel.Payload)
		return h.Handle(ctx, EventID[T](envel.ID), ev.(Event[T]))
	}, params)
	if err == nil {
		slog.Info("subscription created", "subscription", params.DurableName)
	}
	return d, nil
}
