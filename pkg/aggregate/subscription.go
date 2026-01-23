package aggregate

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/alekseev-bro/ddd/internal/typereg"

	"github.com/alekseev-bro/ddd/pkg/qos"
)

type nameForer interface {
	NameFor(in any) (string, error)
}

type SubscribeParams struct {
	DurableName string
	Kind        []string
	AggrID      string
	QoS         qos.QoS
	Reg         nameForer
}

func WithFilterByAggregateID(id ID) ProjOption {
	return func(p *SubscribeParams) {
		p.AggrID = id.String()
	}
}

func WithFilterByEvent[E Evolver[T], T any]() ProjOption {
	var zero any
	t := reflect.TypeFor[E]()
	switch t.Kind() {
	case reflect.Struct:
		zero = new(E)
	case reflect.Pointer:
		var z E
		zero = z
	default:
		panic(fmt.Sprintf("unsupported event type: %s", t.Name()))
	}

	return func(p *SubscribeParams) {
		name, err := p.Reg.NameFor(zero)
		if err != nil {
			slog.Error("filter by event", "error", err)
			panic(err)
		}
		p.Kind = append(p.Kind, name)
	}
}

func WithName[T any](name string) ProjOption {
	return func(p *SubscribeParams) {
		p.DurableName = name
	}
}

func WithQoS[T any](qos qos.QoS) ProjOption {
	return func(p *SubscribeParams) {
		p.QoS = qos
	}
}

func Project[E Evolver[T], T any](ctx context.Context, sub Subscriber[T], h EventHandler[T, E]) (Drainer, error) {

	n := fmt.Sprintf("%s", typereg.TypeNameFrom(h))

	return sub.Subscribe(ctx, &handleEventAdapter[E, T]{h: h}, WithFilterByEvent[E](), WithName[T](n))

}

type handleEventAdapter[E Evolver[T], T any] struct {
	h EventHandler[T, E]
}

func (h *handleEventAdapter[E, T]) HandleEvents(ctx context.Context, event Evolver[T]) error {
	return h.h.HandleEvent(ctx, event.(E))
}

// Subscribe creates a new subscription on aggegate events with the given handler.
func (a *store[T, PT]) Subscribe(ctx context.Context, h EventsHandler[T], opts ...ProjOption) (Drainer, error) {
	dn := typereg.TypeNameFrom(h)

	params := &SubscribeParams{
		DurableName: dn,
		Reg:         a.eventRegistry,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(params)
		}
	}
	d, err := a.es.Subscribe(ctx, func(msg *StoredMsg) error {
		// TODO: implement panic recovery
		ev, err := a.eventSerder.Deserialize(msg.Kind, msg.Body)
		if err != nil {
			panic(err)
		}
		return h.HandleEvents(context.WithValue(ctx, idempKeyCtx, msg.ID), ev)
	}, params)
	if err == nil {
		slog.Info("subscription created", "subscription", params.DurableName)
	}
	return d, nil
}
