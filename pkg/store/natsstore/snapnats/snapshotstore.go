package snapnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/essrv"
	"github.com/alekseev-bro/ddd/pkg/store"

	"github.com/nats-io/nats.go/jetstream"
)

type snapshotStore[T any] struct {
	SnapshotStoreConfig
	kv jetstream.KeyValue
}

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

func NewSnapshotStore[T any](ctx context.Context, js jetstream.JetStream, cfg SnapshotStoreConfig) *snapshotStore[T] {

	ss := &snapshotStore[T]{
		SnapshotStoreConfig: cfg,
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  ss.snapshotBucketName(),
		Storage: jetstream.StorageType(ss.StoreType),
	})
	if err != nil {
		panic(err)
	}

	ss.kv = kv
	return ss
}

func (s *snapshotStore[T]) snapshotBucketName() string {
	return fmt.Sprintf("snapshot-%s", typereg.TypeNameFor[T](typereg.WithDelimiter("-")))
}

func (s *snapshotStore[T]) Save(ctx context.Context, id essrv.ID[T], snap *essrv.Snapshot[T]) error {
	b, err := serde.Serialize(snap)
	if err != nil {
		slog.Warn("snapshot save serialization", "error", err.Error())
		return err
	}
	_, err = s.kv.Put(ctx, id.String(), b)
	return err
}

func (s *snapshotStore[T]) Load(ctx context.Context, id essrv.ID[T]) (*essrv.Snapshot[T], error) {

	v, err := s.kv.Get(ctx, id.String())
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, store.ErrNoSnapshot
		}
		return nil, err
	}

	var snap essrv.Snapshot[T]

	if err := serde.Deserialize(v.Value(), &snap); err != nil {
		slog.Error("load snapshot deserialize", "error", err)
		panic(fmt.Errorf("load snapshot: %w", err))
	}

	return &snap, nil
}
