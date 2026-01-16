package snapnats

import (
	"context"
	"errors"
	"fmt"

	"github.com/alekseev-bro/ddd/pkg/aggregate"

	"github.com/nats-io/nats.go/jetstream"
)

type snapshotStore struct {
	SnapshotStoreConfig
	kv jetstream.KeyValue
}

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

func NewSnapshotStore(ctx context.Context, js jetstream.JetStream, name string, cfg SnapshotStoreConfig) *snapshotStore {

	ss := &snapshotStore{
		SnapshotStoreConfig: cfg,
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  ss.snapshotBucketName(name),
		Storage: jetstream.StorageType(ss.StoreType),
	})
	if err != nil {
		panic(err)
	}

	ss.kv = kv
	return ss
}

func (s *snapshotStore) snapshotBucketName(name string) string {
	return fmt.Sprintf("snapshot-%s", name)
}

func (s *snapshotStore) Save(ctx context.Context, key []byte, value []byte) error {

	_, err := s.kv.Put(ctx, string(key), value)
	return err
}

func (s *snapshotStore) Load(ctx context.Context, key []byte) ([]byte, error) {

	v, err := s.kv.Get(ctx, string(key))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, aggregate.ErrNoSnapshot
		}
		return nil, err
	}

	return v.Value(), nil
}
