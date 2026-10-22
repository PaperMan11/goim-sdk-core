package encoder

import (
	"bytes"
	"encoding/gob"
)

type GobEncoder struct{}

func NewGobEncoder() *GobEncoder {
	return &GobEncoder{}
}

func (e *GobEncoder) Marshal(v interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *GobEncoder) Unmarshal(data []byte, v interface{}) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(v); err != nil {
		return err
	}
	return nil
}

func (e *GobEncoder) Name() string {
	return "gob"
}
