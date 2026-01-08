package essrv

import (
	"time"

	"github.com/alekseev-bro/ddd/internal/typereg"
)

type AggregateConfig struct {
	SnapthotMsgThreshold byte
	SnapshotMaxInterval  time.Duration
}

type RegisteredEvent[T any] func(a *root[T])

func WithEvent[E Evolver[T], T any]() RegisteredEvent[T] {

	return func(a *root[T]) {
		var zero E
		typereg.Register(zero)
	}
}
