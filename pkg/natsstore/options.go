package natsstore

import (
	"time"

	"github.com/alekseev-bro/ddd/internal/driver/snapshot/snapnats"
	"github.com/alekseev-bro/ddd/internal/driver/stream/esnats"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"

	"github.com/nats-io/nats.go/jetstream"
)

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type options[T any, PT aggregate.PRoot[T]] struct {
	esCfg  esnats.EventStreamConfig
	ssCfg  snapnats.SnapshotStoreConfig
	agOpts []aggregate.StoreOption[T, PT]
}

type option[T any, PT aggregate.PRoot[T]] func(c *options[T, PT])

func WithInMemory[T any, PT aggregate.PRoot[T]]() option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg.StoreType = esnats.Memory
		opts.ssCfg.StoreType = snapnats.Memory
	}
}

func WithDeduplication[T any, PT aggregate.PRoot[T]](duration time.Duration) option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg.Deduplication = duration
	}
}

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T any, PT aggregate.PRoot[T]](numMsgs byte, maxInterval time.Duration) option[T, PT] {
	return func(o *options[T, PT]) {
		o.agOpts = append(o.agOpts, aggregate.WithSnapshot[T, PT](numMsgs, maxInterval))
	}
}

func WithEvent[E any, T any, PE interface {
	*E
	aggregate.Evolver[T]
}, PT aggregate.PRoot[T]](name string) option[T, PT] {
	return func(o *options[T, PT]) {
		o.agOpts = append(o.agOpts, aggregate.WithEvent[E, T, PE, PT](name))
	}
}

func WithSnapshot[T any, PT aggregate.PRoot[T]](maxMsgs byte, maxInterval time.Duration) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, aggregate.WithSnapshot[T, PT](maxMsgs, maxInterval))
	}
}
func WithEventCodec[T any, PT aggregate.PRoot[T]](codec codec.Codec) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, aggregate.WithEventCodec[T, PT](codec))
	}
}

func WithSnapshotCodec[T any, PT aggregate.PRoot[T]](codec codec.Codec) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotCodec[T, PT](codec))
	}
}
