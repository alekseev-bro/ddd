package snapnats

import "github.com/alekseev-bro/ddd/pkg/eventstore"

type Option[T any] func(*snapshotStore[T])

func WithInMemory[T any, PT eventstore.Aggregate[T]]() Option[T] {
	return func(ss *snapshotStore[T]) {
		ss.storeType = Memory

	}
}
