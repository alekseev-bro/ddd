package natsstore

import (
	"github.com/alekseev-bro/ddd/pkg/essrv"
	"github.com/nats-io/nats.go/jetstream"
)

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

// type options[T any] struct {
// 	esOpts []esnats.Option
// 	ssOpts []snapnats.Option
// 	agOpts []aggregate.Option[T]
// }

//type option[T any] func(*options[T])

type NatsAggregateConfig struct {
	essrv.AggregateConfig
	StoreType    StoreType
	PartitionNum byte
}

// func WithInMemory[T any]() option[T] {
// 	return func(opts *options[T]) {
// 		opts.ssOpts = append(opts.ssOpts, snapnats.WithInMemory())
// 		opts.esOpts = append(opts.esOpts, esnats.WithInMemory())
// 	}
// }

// // WithSnapshotThreshold sets the threshold for snapshotting.
// // numMsgs is the number of messages to accumulate before snapshotting,
// // and the interval is the minimum time interval between snapshots.
// func WithSnapshotThreshold[T any](numMsgs byte, interval time.Duration) option[T] {
// 	return func(o *options[T]) {
// 		o.agOpts = append(o.agOpts, aggregate.WithSnapshotThreshold[T](numMsgs, interval))
// 	}
// }

// func WithEvent[E aggregate.Evolver[T], T any]() option[T] {

// 	return func(o *options[T]) {
// 		o.agOpts = append(o.agOpts, aggregate.WithEvent[E, T]())
// 	}
// }
