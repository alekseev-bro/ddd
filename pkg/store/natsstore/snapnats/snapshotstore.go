package snapnats

import (
	"context"
	"errors"
	"fmt"

	"github.com/alekseev-bro/ddd/pkg/eventstore"
	"github.com/alekseev-bro/ddd/pkg/store"

	"github.com/nats-io/nats.go/jetstream"
)

type snapshotStore[T any] struct {
	storeType  StoreType
	tname      string
	boundedCtx string
	kv         jetstream.KeyValue
}

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

func NewSnapshotStore[T any](ctx context.Context, js jetstream.JetStream, opts ...Option[T]) *snapshotStore[T] {
	aname, bname := eventstore.AggregateNameFromType[T]()
	ss := &snapshotStore[T]{
		tname:      aname,
		boundedCtx: bname,
	}
	for _, opt := range opts {
		opt(ss)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  ss.snapshotBucketName(),
		Storage: jetstream.StorageType(ss.storeType),
	})
	if err != nil {
		panic(err)
	}

	ss.kv = kv
	return ss
}

func (s *snapshotStore[T]) snapshotBucketName() string {
	return fmt.Sprintf("snapshot-%s-%s", s.boundedCtx, s.tname)
}

func (s *snapshotStore[T]) Save(ctx context.Context, id string, snap []byte) error {
	_, err := s.kv.Put(ctx, id, snap)
	return err
}

func (s *snapshotStore[T]) Load(ctx context.Context, id string) ([]byte, error) {
	v, err := s.kv.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, store.ErrNoSnapshot
		}
		return nil, err
	}

	return v.Value(), nil
}
