package serde

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/alekseev-bro/ddd/pkg/codec"
)

type Creator interface {
	Create(name string) (any, error)
}

func NewSerder[T any](reg Creator, c codec.Codec) *serder[T] {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Interface {
		slog.Error("type T is not an interface")
		panic("type T is not an interface")
	}

	return &serder[T]{
		codec: c,
		reg:   reg,
	}
}

type serder[T any] struct {
	codec codec.Codec
	reg   Creator
}

func (j *serder[T]) Serialize(v T) ([]byte, error) {
	return j.codec.Marshal(v)
}

func (s *serder[T]) Deserialize(t string, b []byte) (T, error) {
	var zero T
	out, err := s.reg.Create(t)
	if err != nil {
		return zero, fmt.Errorf("deserialize: %w", err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return zero, fmt.Errorf("deserialize: %w", err)
	}
	return out.(T), nil
}
