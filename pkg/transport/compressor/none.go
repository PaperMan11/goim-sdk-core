package compressor

type NoneCompressor struct{}

func NewNoneCompressor() *NoneCompressor {
	return &NoneCompressor{}
}

func (c *NoneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (c *NoneCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (c *NoneCompressor) Name() string {
	return "none"
}
