package snapnats

type SnapshotStoreConfig struct {
	StoreType StoreType
}

// type Option func(*SnapshotStoreConfig)

// func WithInMemory() Option {
// 	return func(ss *SnapshotStoreConfig) {
// 		ss.StoreType = Memory

// 	}
// }
