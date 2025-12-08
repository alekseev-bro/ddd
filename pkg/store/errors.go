package store

import "errors"

var (
	ErrNoAggregate = errors.New("no aggregate messages")
	ErrNoSnapshot  = errors.New("no snapshot")
)
