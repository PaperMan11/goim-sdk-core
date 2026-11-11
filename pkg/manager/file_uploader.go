package manager

import (
	"context"
	"io"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// UploadOptions 上传选项（FileUploader.Upload 参数）
type UploadOptions struct {
	// Name S3 对象 key（必填，如 "photos/2026/abc.jpg"）
	Name string
	// Size 文件总大小（字节，必填）
	Size int64
	// ContentType MIME 类型（可选，如 "image/jpeg"，RustFS 会据此推断）
	ContentType string
	// Hash 文件 hash（可选，服务端用于幂等/校验）
	Hash string
	// Cause 上传原因（可选，如 "sendMsg"，服务端日志分类）
	Cause string
	// URLPrefix 可选 URL 前缀（与 AccessURL 对齐）
	URLPrefix string

	// ForceMultipart 强制走 Multipart 分片上传（大文件场景或需要断点续传时）
	// 默认按 MultipartThreshold 自动选择
	ForceMultipart bool
	// ForceFormData 强制走 FormData 小文件直传（覆盖默认路径选择）
	ForceFormData bool

	// Concurrency Multipart 分片上传并发度（默认 DefaultUploadConcurrency=4）
	Concurrency int
	// PartSize Multipart 分片大小（默认由服务端 PartSize 推荐）
	// 设 0 表示自动调用 Third.PartSize 获取推荐值
	PartSize int64
	// MaxParts Multipart 最大分片数（默认由服务端 PartLimit 返回）
	MaxParts int32
	// PartRetry 单片 PUT 重试次数（默认 3）
	PartRetry int

	// OnProgress 进度回调（uploaded 已上传字节，total 总字节）
	// 多线程调用，注意加锁
	OnProgress func(uploaded, total int64)

	// —— 断点续传 ——

	// Resume 断点续传状态（上传取消时 UploadResult.ResumeState 会返回）。
	// 非空时跳过 InitiateMultipartUpload，直接用已有的 uploadID + completedParts
	// 补传剩余分片。
	Resume *ResumeState
}

// ResumeState 断点续传恢复状态（上传取消时返回给调用方持久化）
//
// 持久化建议：存 App 本地 DB / JSON 文件，key = fileHash + name 组合
// 恢复时校验：文件是否还是原来的（Hash 匹配 + Size 匹配）
type ResumeState struct {
	// UploadID Multipart 上传会话 ID（服务端保持 7 天）
	UploadID string
	// PartSize 实际使用的分片大小（恢复时必须和原来一致）
	PartSize int64
	// CompletedParts 已成功上传的分片列表（含 PartNumber + ETag）
	// 恢复时跳过这些分片的 PUT
	CompletedParts []model.CompletedPart
}

// UploadResult 上传结果
type UploadResult struct {
	// URL 最终下载 URL（CompleteMultipartUpload 或 CompleteFormData 返回）。
	// 上传被取消时为空（上传未完成）。
	URL string
	// Multipart 是否走了 Multipart 分片上传
	Multipart bool
	// UploadID Multipart 上传 ID
	UploadID string
	// PartSize 实际使用的分片大小（Multipart 路径；FormData 为 0）
	PartSize int64
	// CompletedParts 已成功上传的分片列表（不管上传是否完成都返回，供取消时持久化）
	CompletedParts []model.CompletedPart
	// ResumeState 断点续传状态（上传被取消时非空，UploadID + CompletedParts 全量）
	ResumeState *ResumeState
}

// FileUploader 第三方存储一站式上传引擎
//
// 职责：封装 ThirdMgr 签名控制 API + 并发文件字节传输（PUT 到预签名 URL /
// FormData POST），App 层只需调 Upload(ctx, reader, opts) → URL。
//
// 与 ThirdMgr 的关系：FileUploader 组合 ThirdMgr 作为签名层，不直接持有 HTTPClient。
// 两者独立接口——App 可以只用 ThirdMgr 手动管理上传，也可以用 FileUploader 一站式。
//
// 自动路径选择（ForceMultipart / ForceFormData 未设置时）：
//
//	size >= MultipartThreshold → Multipart 分片上传（init → 并发 PUT 分片 → complete）
//	size <  MultipartThreshold → FormData 直传（init → POST multipart/form-data → complete）
//
// 取消：ctx cancel 时终止所有并发 PUT，**保留服务端已上传分片**（不调 AbortMultipartUpload）。
// UploadResult.ResumeState 会返回 uploadID + 已完成分片列表，供调用方持久化断点续传。
// 如果不想续传而是彻底放弃，直接调 Third().AbortMultipartUpload() 或 FileUploader.Abort()。
type FileUploader interface {
	// Upload 一站式上传：自动选择路径、并发 PUT、重试、进度、取消、断点续传
	Upload(ctx context.Context, reader io.Reader, opts *UploadOptions) (*UploadResult, error)
	// Abort 彻底放弃 Multipart 上传（清理服务端已上传分片），不可续传
	Abort(ctx context.Context, uploadID, name, urlPrefix string) error
}

// 默认常量
const (
	// DefaultMultipartThreshold 分片上传阈值（>= 此值走 Multipart，< 走 FormData）
	// 8MB 是 S3 FormData POST 的常见安全上限（大文件 FormData 容易超时）
	DefaultMultipartThreshold int64 = 8 * 1024 * 1024
	// DefaultUploadConcurrency 默认并发度
	DefaultUploadConcurrency = 4
	// DefaultPartRetry 默认单片重试次数
	DefaultPartRetry = 3
	// DefaultHTTPPutTimeout 单片 PUT 超时（大文件分片可能耗时长，留 10min）
	DefaultHTTPPutTimeout = 10 * 60 * time.Second
)
