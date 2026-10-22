package encoder

import "encoding/json"

type JsonEncoder struct{}

func NewJsonEncoder() *JsonEncoder {
	return &JsonEncoder{}
}

func (s *JsonEncoder) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (s *JsonEncoder) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (s *JsonEncoder) Name() string {
	return "json"
}
