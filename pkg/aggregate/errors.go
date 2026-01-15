package aggregate

import "errors"

var (
	ErrNoSnapshot             = errors.New("no snapshot")
	ErrNoAggregate            = errors.New("no aggregate messages")
	ErrNoStore                = errors.New("no store exists")
	ErrAggregateNotExists     = errors.New("aggregate is not created yet")
	ErrAggregateDeleted       = errors.New("aggregate is deleted")
	ErrAggregateAlreadyExists = errors.New("aggregate already exists")
)
