package natsstore

import (
	"context"

	"github.com/alekseev-bro/ddd/pkg/eventstore"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"

	"github.com/nats-io/nats.go/jetstream"
)

func NewAggregate[T any, PT eventstore.Aggregate[T]](ctx context.Context, js jetstream.JetStream, opts ...option[T]) eventstore.EventStore[T] {
	op := options[T]{}
	for _, opt := range opts {
		opt(&op)
	}
	es := esnats.NewEventStream[T](ctx, js, op.esOpts...)
	ss := snapnats.NewSnapshotStore[T](ctx, js, op.ssOpts...)
	return eventstore.New(ctx, es, ss, op.agOpts...)
}
