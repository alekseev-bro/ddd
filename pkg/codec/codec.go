package codec

import "encoding/json"

var JSON = jsonCodec{}

type jsonCodec struct{}

func (j jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (j jsonCodec) Unmarshal(b []byte, out any) error {
	return json.Unmarshal(b, out)
}

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, out any) error
}
