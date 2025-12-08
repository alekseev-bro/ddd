package domain

import (
	"context"

	reg "github.com/alekseev-bro/ddd/internal/registry"
)

type OrderingType uint

const (
	Ordered OrderingType = iota
	Unordered
)

type QoSType uint

const (
	AtLeastOnce QoSType = iota
	AtMostOnce
)

type SubscribeParams struct {
	DurableName string
	Ordering    OrderingType
	Kind        []string
	aggrID      string
	QoS         QoSType
}

func (s *SubscribeParams) AggrID() string {
	if s.aggrID != "" {
		return s.aggrID
	}

	return "*"

}

func FilterByAggregateID[T any](id ID[T]) ProjOption {
	return func(p *SubscribeParams) {
		p.aggrID = id.String()
	}
}

func WithUnordered() ProjOption {
	return func(p *SubscribeParams) {
		p.Ordering = Unordered
	}
}

func FilterByEvent[E Event[T], T any]() ProjOption {
	return func(p *SubscribeParams) {
		var ev E
		p.Kind = append(p.Kind, reg.TypeNameFrom(ev))
	}
}

func WithName(name string) ProjOption {
	return func(p *SubscribeParams) {
		p.DurableName = name
	}
}

func WithAtMostOnce() ProjOption {
	return func(p *SubscribeParams) {
		p.QoS = AtMostOnce
	}
}

type EventHandler[T any] interface {
	Handle(ctx context.Context, eventID ID[Event[T]], event Event[T]) error
}

func (a *aggregate[T]) Project(ctx context.Context, h EventHandler[T], opts ...ProjOption) ([]Drainer, error) {
	params := &SubscribeParams{
		DurableName: reg.TypeNameFrom(h),
		Ordering:    Ordered,
		QoS:         AtLeastOnce,
	}
	for _, opt := range opts {
		opt(params)
	}

	return a.es.Subscribe(ctx, func(envel *Envelope) error {
		ev := a.tr.GetType(envel.Kind, envel.Payload)
		return h.Handle(ctx, ID[Event[T]](envel.ID.String()), ev.(Event[T]))
	}, params)
}
