package aggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typereg"
)

type AggregateConfig struct {
	SnapthotMsgThreshold byte
	SnapshotMaxInterval  time.Duration
}

type RegisteredEvent[T Root, PT PRoot[T]] func(a *store[T, PT])

func WithEvent[E Evolver[T], T Root, PT PRoot[T]](name string) RegisteredEvent[T, PT] {

	return func(a *store[T, PT]) {
		var zero E
		ct := func(b []byte) any {
			serde.Deserialize(b, &zero)
			return zero
		}
		if name == "" {
			name = typereg.TypeNameFor[E]()
		}
		typereg.Register[E](name, ct)
	}
}
