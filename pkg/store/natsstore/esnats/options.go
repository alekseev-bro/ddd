package esnats

type Option[T any] func(*eventStream[T]) error

func WithPartitions[T any](partitions byte) Option[T] {
	return func(es *eventStream[T]) error {
		es.partnum = partitions
		return nil
	}
}

func WithInMemory[T any]() Option[T] {
	return func(es *eventStream[T]) error {
		es.storeType = Memory
		return nil
	}
}
