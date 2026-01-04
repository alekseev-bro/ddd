package natsstore

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/eventstore"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"
)

type options[T any] struct {
	esOpts []esnats.Option[T]
	ssOpts []snapnats.Option[T]
	agOpts []eventstore.Option[T]
}

type option[T any] func(*options[T])

func WithInMemory[T any]() option[T] {
	return func(opts *options[T]) {
		opts.ssOpts = append(opts.ssOpts, snapnats.WithInMemory[T]())
		opts.esOpts = append(opts.esOpts, esnats.WithInMemory[T]())
	}
}

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T any](numMsgs byte, interval time.Duration) option[T] {
	return func(o *options[T]) {
		o.agOpts = append(o.agOpts, eventstore.WithSnapshotThreshold[T](numMsgs, interval))
	}
}

func WithEvent[E eventstore.Event[T], T any, PT eventstore.Aggregate[T]]() option[T] {

	return func(o *options[T]) {
		o.agOpts = append(o.agOpts, eventstore.WithEvent[E, T]())
	}
}
