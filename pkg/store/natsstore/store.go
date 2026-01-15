package natsstore

import (
	"context"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"

	"github.com/nats-io/nats.go/jetstream"
)

func NewStore[T aggregate.Root, PT aggregate.PRoot[T]](ctx context.Context, js jetstream.JetStream, cfg NatsAggregateConfig, opts ...aggregate.RegisteredEvent[T, PT]) aggregate.Store[T, PT] {
	esCfg := esnats.EventStreamConfig{
		StoreType:    esnats.StoreType(cfg.StoreType),
		PartitionNum: cfg.PartitionNum,
	}
	ssCfg := snapnats.SnapshotStoreConfig{
		StoreType: snapnats.StoreType(cfg.StoreType),
	}
	es := esnats.NewEventStream[T](ctx, js, esCfg)
	ss := snapnats.NewSnapshotStore[T](ctx, js, ssCfg)
	return aggregate.NewStore(ctx, es, ss, cfg.AggregateConfig, opts...)
}
