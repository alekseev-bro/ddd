package eventstore

import (
	"time"

	"github.com/alekseev-bro/ddd/internal/typereg"
)

type Option[T any] func(a *eventStore[T])

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T any](numMsgs byte, interval time.Duration) Option[T] {
	return func(a *eventStore[T]) {
		a.snapshotThreshold.numMsgs = numMsgs
		a.snapshotThreshold.interval = interval
	}
}

func WithEvent[E Event[T], T any]() Option[T] {

	return func(a *eventStore[T]) {
		var zero E
		typereg.Register(zero)
	}
}
