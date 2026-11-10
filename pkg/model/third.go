package model

// ThirdParty 第三方存储相关请求/响应结构体
// 对齐 goim im-rpc/third proto + im-api HTTP JSON 风格

// KeyValues 对齐 proto KeyValues（S3 预签名 URL 的 query/header 条目）
type KeyValues struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// SignPart 单个分片的预签名上传信息
type SignPart struct {
	PartNumber int32       `json:"partNumber"`
	URL        string      `json:"url"`
	Query      []KeyValues `json:"query"`
	Header     []KeyValues `json:"header"`
}

// AuthSignParts AuthSign 返回的统一签名容器（URL + query + header + 分片签名）
type AuthSignParts struct {
	URL    string      `json:"url"`
	Query  []KeyValues `json:"query"`
	Header []KeyValues `json:"header"`
	Parts  []SignPart  `json:"parts"`
}

// PartLimitResp 分片上传参数约束（服务端根据 RustFS 能力返回）
type PartLimitResp struct {
	MinPartSize int64 `json:"minPartSize"`
	MaxPartSize int64 `json:"maxPartSize"`
	MaxNumSize  int32 `json:"maxNumSize"`
}

// PartSizeReq 推荐分片大小请求
type PartSizeReq struct {
	Size int64 `json:"size"`
}

// PartSizeResp 推荐分片大小（客户端按此切分）
type PartSizeResp struct {
	Size int64 `json:"size"`
}

// InitiateMultipartUploadReq 分片上传初始化请求
type InitiateMultipartUploadReq struct {
	Hash        string `json:"hash,omitempty"`        // 文件 hash（可选，用于幂等/校验）
	Size        int64  `json:"size"`                 // 文件总大小（字节）
	PartSize    int64  `json:"partSize"`             // 分片大小（字节）
	MaxParts    int32  `json:"maxParts"`             // 最大分片数
	Cause       string `json:"cause,omitempty"`       // 上传原因（如 sendMsg）
	Name        string `json:"name"`                 // 对象 key（S3 object key）
	ContentType string `json:"contentType,omitempty"` // MIME 类型
	URLPrefix   string `json:"urlPrefix,omitempty"`   // 可选 URL 前缀，与 AccessURL 对齐
}

// UploadInfo 分片上传初始化返回的上传上下文
type UploadInfo struct {
	UploadID   string        `json:"uploadID"`
	PartSize   int64         `json:"partSize"`
	Sign       AuthSignParts `json:"sign"`
	ExpireTime int64         `json:"expireTime"` // Unix 秒
}

// InitiateMultipartUploadResp 分片上传初始化响应
type InitiateMultipartUploadResp struct {
	URL    string     `json:"url"`
	Upload UploadInfo `json:"upload"`
}

// AuthSignReq 续签请求（预签名 URL 过期后调用，或提前申请多片签名）
type AuthSignReq struct {
	UploadID   string   `json:"uploadID"`
	PartNumbers []int32 `json:"partNumbers"`
	Name       string   `json:"name,omitempty"`
	URLPrefix  string   `json:"urlPrefix,omitempty"`
}

// AuthSignResp 续签响应（含新的预签名 URL + 分片签名）
type AuthSignResp struct {
	URL        string      `json:"url"`
	Query      []KeyValues `json:"query"`
	Header     []KeyValues `json:"header"`
	Parts      []SignPart  `json:"parts"`
	ExpireTime int64       `json:"expireTime"`
}

// CompletedPart 已上传分片的完成信息
type CompletedPart struct {
	PartNumber int32  `json:"partNumber"`
	ETag       string `json:"eTag"`
}

// CompleteMultipartUploadReq 分片上传完成请求
type CompleteMultipartUploadReq struct {
	UploadID    string          `json:"uploadID"`
	Parts       []CompletedPart `json:"parts"`
	Name        string          `json:"name"`
	ContentType string          `json:"contentType,omitempty"`
	Cause       string          `json:"cause,omitempty"`
	URLPrefix   string          `json:"urlPrefix,omitempty"`
}

// CompleteMultipartUploadResp 分片上传完成响应
type CompleteMultipartUploadResp struct {
	URL string `json:"url"`
}

// AbortMultipartUploadReq 分片上传放弃请求
type AbortMultipartUploadReq struct {
	UploadID  string `json:"uploadID"`
	Name      string `json:"name,omitempty"`
	URLPrefix string `json:"urlPrefix,omitempty"`
}

// AccessURLReq 下载 URL 请求
type AccessURLReq struct {
	Name      string            `json:"name"`
	Query     map[string]string `json:"query,omitempty"`
	URLPrefix string            `json:"urlPrefix,omitempty"`
}

// AccessURLResp 下载 URL 响应（预签名，有过期时间）
type AccessURLResp struct {
	URL        string `json:"url"`
	ExpireTime int64  `json:"expireTime"`
}

// InitiateFormDataReq FormData 小文件直传初始化请求
type InitiateFormDataReq struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType,omitempty"`
	Group       string `json:"group,omitempty"`
	Millisecond int64  `json:"millisecond,omitempty"`
	Absolute    bool   `json:"absolute,omitempty"`
}

// InitiateFormDataResp FormData 初始化响应（含预签名表单字段）
type InitiateFormDataResp struct {
	ID           string            `json:"id"`
	URL          string            `json:"url"`
	File         string            `json:"file"`
	Header       []KeyValues       `json:"header"`
	FormData     map[string]string `json:"formData"`
	Expires      int64             `json:"expires"`
	SuccessCodes []int32           `json:"successCodes"`
}

// CompleteFormDataReq FormData 上传完成请求
type CompleteFormDataReq struct {
	ID        string `json:"id"`
	URLPrefix string `json:"urlPrefix,omitempty"`
}

// CompleteFormDataResp FormData 上传完成响应
type CompleteFormDataResp struct {
	URL string `json:"url"`
}

// DeleteOutdatedDataReq 过期数据清理请求（管理接口）
type DeleteOutdatedDataReq struct {
	ExpireTime  int64    `json:"expireTime"`
	ObjectGroup []string `json:"objectGroup,omitempty"`
	Limit       int32    `json:"limit,omitempty"`
}

// DeleteOutdatedDataResp 清理结果
type DeleteOutdatedDataResp struct {
	Count int32 `json:"count"`
}
