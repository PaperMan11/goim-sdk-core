package compressor

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Name() string
}

type CompressorFactory struct {
	compressors map[string]Compressor
	defaultType string
}

func NewCompressorFactory() *CompressorFactory {
	factory := &CompressorFactory{
		compressors: make(map[string]Compressor),
		defaultType: "none",
	}
	factory.Register("none", NewNoneCompressor())
	factory.Register("gzip", NewGzipCompressor())
	factory.Register("zlib", NewZlibCompressor())
	return factory
}

func (f *CompressorFactory) Register(compressionType string, compressor Compressor) {
	f.compressors[compressionType] = compressor
}

func (f *CompressorFactory) GetCompressor(compressionType string) Compressor {
	if compressor, ok := f.compressors[compressionType]; ok {
		return compressor
	}
	return f.compressors[f.defaultType]
}

var globalCompressorFactory = NewCompressorFactory()

func GetCompressor(compressionType string) Compressor {
	return globalCompressorFactory.GetCompressor(compressionType)
}

func RegisterCompressor(compressionType string, compressor Compressor) {
	globalCompressorFactory.Register(compressionType, compressor)
}
