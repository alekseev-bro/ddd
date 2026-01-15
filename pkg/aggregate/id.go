package aggregate

import (
	"database/sql/driver"

	"github.com/google/uuid"
)

func NewID() ID {
	a, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return ID(a)
}

type ID uuid.UUID

func (i ID) IsZero() bool {
	return uuid.UUID(i) == uuid.Nil
}

func (i ID) String() string {
	return uuid.UUID(i).String()
}

func (id *ID) UnmarshalText(data []byte) error {

	return (*uuid.UUID)(id).UnmarshalText(data)
}

func (i ID) UUID() uuid.UUID {

	return uuid.UUID(i)
}

// 2. Restore JSON Marshaling (if needed)
func (id ID) MarshalText() ([]byte, error) {
	return uuid.UUID(id).MarshalText()
}

func (id *ID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(id).UnmarshalBinary(data)
}
func (id ID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(id).MarshalBinary()
}

// 3. Restore Database Compatibility (SQL driver)
func (id ID) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

func (id *ID) Scan(src any) error {
	return (*uuid.UUID)(id).Scan(src)
}
