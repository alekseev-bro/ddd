package aggregate

import "time"

type Option[T any] func(a *Root[T])

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T any](numMsgs byte, interval time.Duration) Option[T] {
	return func(a *Root[T]) {
		a.snapshotThreshold.numMsgs = numMsgs
		a.snapshotThreshold.interval = interval
	}
}
