package esnats

type EventStreamConfig struct {
	StoreType    StoreType
	PartitionNum byte
}

// type Option func(*EventStreamConfig)

// func WithPartitions(partitions byte) Option {
// 	return func(es *EventStreamConfig) {
// 		es.PartitionNum = partitions

// 	}
// }

// func WithInMemory() Option {
// 	return func(es *EventStreamConfig) {
// 		es.StoreType = Memory

// 	}
// }
