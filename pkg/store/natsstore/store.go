package natsstore

import (
	"context"

	"github.com/alekseev-bro/ddd/pkg/events"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"

	"github.com/nats-io/nats.go/jetstream"
)

func NewStore[T any](ctx context.Context, js jetstream.JetStream, cfg NatsAggregateConfig, opts ...events.RegisteredEvent[T]) events.Store[T] {
	esCfg := esnats.EventStreamConfig{
		StoreType:    esnats.StoreType(cfg.StoreType),
		PartitionNum: cfg.PartitionNum,
	}
	ssCfg := snapnats.SnapshotStoreConfig{
		StoreType: snapnats.StoreType(cfg.StoreType),
	}
	es := esnats.NewEventStream[T](ctx, js, esCfg)
	ss := snapnats.NewSnapshotStore[T](ctx, js, ssCfg)
	return events.NewStore(ctx, es, ss, cfg.AggregateConfig, opts...)
}
