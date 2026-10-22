package compressor

import (
	"bytes"
	"compress/zlib"
	"io"
)

type ZlibCompressor struct{}

func NewZlibCompressor() *ZlibCompressor {
	return &ZlibCompressor{}
}

func (c *ZlibCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *ZlibCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (c *ZlibCompressor) Name() string {
	return "zlib"
}
