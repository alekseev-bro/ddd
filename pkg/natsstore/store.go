package natsstore

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alekseev-bro/ddd/internal/driver/snapshot/snapnats"
	"github.com/alekseev-bro/ddd/internal/driver/stream/esnats"
	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"

	"github.com/nats-io/nats.go/jetstream"
)

func NewStore[T any, PT aggregate.PRoot[T]](ctx context.Context, js jetstream.JetStream, opts ...option[T, PT]) aggregate.Store[T, PT] {
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		panic("type T is not a struct")
	}
	cfg := &options[T, PT]{}
	for _, opt := range opts {
		opt(cfg)
	}
	es := esnats.NewEventStream(ctx, js, fmt.Sprintf("aggregate-%s", typereg.TypeNameFor[T]()), cfg.esCfg)
	ss := snapnats.NewSnapshotStore(ctx, js, typereg.TypeNameFor[T](typereg.WithDelimiter("-")), cfg.ssCfg)
	return aggregate.NewStore(ctx, es, ss, cfg.agOpts...)
}
