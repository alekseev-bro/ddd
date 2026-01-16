package aggregate

import (
	"fmt"
	"reflect"
	"time"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/pkg/codec"
)

type storeConfig struct {
	SnapthotMsgThreshold byte
	SnapshotMaxInterval  time.Duration
}

type StoreOption[T any, PT PRoot[T]] func(a *store[T, PT])

func WithEvent[E any, T any, PE interface {
	*E
	Evolver[T]
}, PT PRoot[T]](name string) StoreOption[T, PT] {

	if reflect.TypeFor[E]().Kind() != reflect.Struct {
		panic(fmt.Sprintf("event '%s' must be a struct and not a pointer", reflect.TypeFor[E]().Elem().Name()))
	}
	return func(a *store[T, PT]) {

		if name == "" {
			name = reflect.TypeFor[E]().Name()
		}

		a.eventRegistry.Register(name, func() any { return PE(new(E)) })
	}
}

func WithSnapshot[T any, PT PRoot[T]](maxMsgs byte, maxInterval time.Duration) StoreOption[T, PT] {
	return func(a *store[T, PT]) {
		a.storeConfig = storeConfig{
			SnapthotMsgThreshold: maxMsgs,
			SnapshotMaxInterval:  maxInterval,
		}
	}
}
func WithEventCodec[T any, PT PRoot[T]](codec codec.Codec) StoreOption[T, PT] {
	return func(a *store[T, PT]) {
		a.eventSerder = serde.NewSerder[Evolver[T]](a.eventRegistry, codec)
	}
}

func WithSnapshotCodec[T any, PT PRoot[T]](codec codec.Codec) StoreOption[T, PT] {
	return func(a *store[T, PT]) {
		a.snapshotCodec = codec
	}
}
