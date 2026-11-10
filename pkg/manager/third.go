package manager

import (
	"context"

	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// ThirdMgr 第三方存储（RustFS/MinIO/AWS S3）签名类 HTTP API 薄封装
//
// 职责边界：
//   - SDK 封装所有 im-api /third/... 控制类 HTTP 调用（签名、初始化、完成、续签）
//   - 文件字节传输（PUT 到预签名 URL、FormData POST）由 App / 可选上传引擎负责
//   - SDK 不持有 RustFS AccessKey/SecretKey，预签名 URL 由服务端鉴权后签发
//
// 典型上传流程（Multipart，App 层）：
//   1. PartSize(ctx, size) → 推荐分片大小
//   2. InitiateMultipartUpload(ctx, req) → uploadID + 预签名 URL 列表
//   3. 并发 PUT 分片到各自预签名 URL（SDK 可选封装 FileUploader）
//   4. CompleteMultipartUpload(ctx, req) → 最终下载 URL
//
// 典型上传流程（FormData 小文件，App 层）：
//   1. InitiateFormData(ctx, req) → 预签名表单字段
//   2. FormData POST 到指定 URL
//   3. CompleteFormData(ctx, req) → 最终下载 URL
//
// 下载：AccessURL(ctx, req) → 预签名下载 URL（HTTP GET）
type ThirdMgr interface {
	// PartLimit 分片上传参数约束（Min/Max 分片大小、最大分片数）
	PartLimit(ctx context.Context) (*model.PartLimitResp, error)
	// PartSize 根据文件总大小推荐分片大小
	PartSize(ctx context.Context, size int64) (*model.PartSizeResp, error)
	// InitiateMultipartUpload 分片上传初始化（返回 uploadID + 预签名 URL）
	InitiateMultipartUpload(ctx context.Context, req *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error)
	// AuthSign 续签（预签名 URL 过期后重新申请，或提前申请多片签名）
	AuthSign(ctx context.Context, req *model.AuthSignReq) (*model.AuthSignResp, error)
	// CompleteMultipartUpload 分片上传完成（通知服务端合并分片）
	CompleteMultipartUpload(ctx context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error)
	// AbortMultipartUpload 放弃分片上传（服务端清理已上传分片）
	AbortMultipartUpload(ctx context.Context, req *model.AbortMultipartUploadReq) error
	// AccessURL 获取下载预签名 URL
	AccessURL(ctx context.Context, req *model.AccessURLReq) (*model.AccessURLResp, error)
	// InitiateFormData FormData 小文件直传初始化（返回预签名表单字段）
	InitiateFormData(ctx context.Context, req *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error)
	// CompleteFormData FormData 上传完成
	CompleteFormData(ctx context.Context, req *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error)
	// DeleteOutdatedData 清理过期数据（管理接口，需 admin 权限）
	DeleteOutdatedData(ctx context.Context, req *model.DeleteOutdatedDataReq) (*model.DeleteOutdatedDataResp, error)
}
