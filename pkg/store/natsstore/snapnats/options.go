package snapnats

type option[T any] func(*snapshotStore[T])

func WithInMemory[T any]() option[T] {
	return func(ss *snapshotStore[T]) {
		ss.storeType = Memory

	}
}
