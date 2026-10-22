package encoder

type Encoder interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	Name() string
}

type EncoderFactory struct {
	serializers map[string]Encoder
	defaultType string
}

func NewEncoderFactory() *EncoderFactory {
	factory := &EncoderFactory{
		serializers: make(map[string]Encoder),
		defaultType: "normal",
	}
	factory.Register("normal", NewJsonEncoder())
	return factory
}

func (f *EncoderFactory) Register(sdkType string, encoder Encoder) {
	f.serializers[sdkType] = encoder
}

func (f *EncoderFactory) GetEncoder(sdkType string) Encoder {
	if encoder, ok := f.serializers[sdkType]; ok {
		return encoder
	}
	return f.serializers[f.defaultType]
}

var globalEncoderFactory = NewEncoderFactory()

func GetEncoder(sdkType string) Encoder {
	return globalEncoderFactory.GetEncoder(sdkType)
}

func RegisterEncoder(sdkType string, encoder Encoder) {
	globalEncoderFactory.Register(sdkType, encoder)
}
