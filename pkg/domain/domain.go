package domain

import (
	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
)

func RegisterEvent[E aggregate.Event[T], T any]() {
	var ev E
	typereg.Register(ev)
}
