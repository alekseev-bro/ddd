package serde

import "encoding/json"

type Serder interface {
	Serialize(v any) ([]byte, error)
	Deserialize(b []byte, out any) error
}

func NewDefaultSerder() *defaultSerder {
	return &defaultSerder{}
}

type defaultSerder struct{}

func (defaultSerder) Serialize(v any) ([]byte, error) {

	return json.Marshal(v)
}

func (defaultSerder) Deserialize(b []byte, out any) error {

	return json.Unmarshal(b, out)
}
