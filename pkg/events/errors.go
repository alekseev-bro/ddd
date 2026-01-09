package events

import "errors"

var (
	ErrNoSnapshot  = errors.New("no snapshot")
	ErrNoAggregate = errors.New("no aggregate messages")
)
