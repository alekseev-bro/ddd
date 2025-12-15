package natsstore

import (
	"context"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"

	"github.com/nats-io/nats.go/jetstream"
)

func NewAggregate[T any](ctx context.Context, js jetstream.JetStream, opts ...option[T]) *aggregate.Root[T] {
	op := options[T]{}
	for _, opt := range opts {
		opt(&op)
	}
	es := esnats.NewEventStream[T](ctx, js, op.esOpts...)
	ss := snapnats.NewSnapshotStore[T](ctx, js, op.ssOpts...)
	return aggregate.New[T](ctx, es, ss, op.agOpts...)
}
