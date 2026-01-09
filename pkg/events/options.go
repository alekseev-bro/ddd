package events

import (
	"time"

	"github.com/alekseev-bro/ddd/internal/typereg"
)

type AggregateConfig struct {
	SnapthotMsgThreshold byte
	SnapshotMaxInterval  time.Duration
}

type RegisteredEvent[T any] func(a *store[T])

func WithEvent[E Evolver[T], T any]() RegisteredEvent[T] {

	return func(a *store[T]) {
		var zero E
		typereg.Register(zero)
	}
}
