package eventstore

import (
	"database/sql/driver"

	"github.com/google/uuid"
)

type Aggregate[T any] interface {
	*T
}

type ID[T any] uuid.UUID

func (i ID[T]) String() string {
	return uuid.UUID(i).String()
}

func (id *ID[T]) UnmarshalText(data []byte) error {
	return (*uuid.UUID)(id).UnmarshalText(data)
}

func (i ID[T]) UUID() uuid.UUID {
	return uuid.UUID(i)
}

// 2. Restore JSON Marshaling (if needed)
func (id ID[T]) MarshalText() ([]byte, error) {
	return uuid.UUID(id).MarshalText()
}

func (id *ID[T]) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(id).UnmarshalBinary(data)
}
func (id ID[T]) MarshalBinary() ([]byte, error) {
	return uuid.UUID(id).MarshalBinary()
}

// 3. Restore Database Compatibility (SQL driver)
func (id ID[T]) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

func (id *ID[T]) Scan(src any) error {
	return (*uuid.UUID)(id).Scan(src)
}
