package manager

import (
	"context"
	"fmt"
	"net/url"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

// DefaultThirdMgr ThirdMgr 默认实现（纯 HTTP 薄封装，只调签名类控制 API）
//
// 与 DefaultGroupMgr / DefaultMsgMgr 的区别：ThirdMgr 不持有 eventbus，
// 因为第三方存储上传/下载操作不触发 goim 事件推送——SDK 不需要内部 Publish，
// App 也不通过 eventbus 订阅第三方存储事件。
type DefaultThirdMgr struct {
	http   transport.HTTPClient
	logger logger.Logger
}

// NewThirdMgr 构造 ThirdMgr
func NewThirdMgr(http transport.HTTPClient, l logger.Logger) *DefaultThirdMgr {
	return &DefaultThirdMgr{http: http, logger: l}
}

// PartLimit 分片上传参数约束
func (m *DefaultThirdMgr) PartLimit(ctx context.Context) (*model.PartLimitResp, error) {
	var resp model.PartLimitResp
	if err := m.http.Post(ctx, "/third/part_limit", struct{}{}, &resp); err != nil {
		return nil, fmt.Errorf("third PartLimit: %w", err)
	}
	return &resp, nil
}

// PartSize 推荐分片大小
func (m *DefaultThirdMgr) PartSize(ctx context.Context, size int64) (*model.PartSizeResp, error) {
	var resp model.PartSizeResp
	if err := m.http.Post(ctx, "/third/part_size", model.PartSizeReq{Size: size}, &resp); err != nil {
		return nil, fmt.Errorf("third PartSize: %w", err)
	}
	return &resp, nil
}

// InitiateMultipartUpload 分片上传初始化
func (m *DefaultThirdMgr) InitiateMultipartUpload(ctx context.Context, req *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
	var resp model.InitiateMultipartUploadResp
	if err := m.http.Post(ctx, "/third/multipart/init", req, &resp); err != nil {
		return nil, fmt.Errorf("third InitiateMultipartUpload: %w", err)
	}
	return &resp, nil
}

// AuthSign 续签
func (m *DefaultThirdMgr) AuthSign(ctx context.Context, req *model.AuthSignReq) (*model.AuthSignResp, error) {
	var resp model.AuthSignResp
	if err := m.http.Post(ctx, "/third/multipart/sign", req, &resp); err != nil {
		return nil, fmt.Errorf("third AuthSign: %w", err)
	}
	return &resp, nil
}

// CompleteMultipartUpload 分片上传完成
func (m *DefaultThirdMgr) CompleteMultipartUpload(ctx context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
	var resp model.CompleteMultipartUploadResp
	if err := m.http.Post(ctx, "/third/multipart/complete", req, &resp); err != nil {
		return nil, fmt.Errorf("third CompleteMultipartUpload: %w", err)
	}
	return &resp, nil
}

// AbortMultipartUpload 放弃分片上传
func (m *DefaultThirdMgr) AbortMultipartUpload(ctx context.Context, req *model.AbortMultipartUploadReq) error {
	if err := m.http.Post(ctx, "/third/multipart/abort", req, nil); err != nil {
		return fmt.Errorf("third AbortMultipartUpload: %w", err)
	}
	return nil
}

// AccessURL 获取下载预签名 URL（GET，query 参数）
func (m *DefaultThirdMgr) AccessURL(ctx context.Context, req *model.AccessURLReq) (*model.AccessURLResp, error) {
	query := urlValuesFromMap(req.Query)
	if req.Name != "" {
		query.Set("name", req.Name)
	}
	if req.URLPrefix != "" {
		query.Set("urlPrefix", req.URLPrefix)
	}
	var resp model.AccessURLResp
	if err := m.http.Get(ctx, "/third/access_url", query, &resp); err != nil {
		return nil, fmt.Errorf("third AccessURL: %w", err)
	}
	return &resp, nil
}

// InitiateFormData FormData 小文件直传初始化
func (m *DefaultThirdMgr) InitiateFormData(ctx context.Context, req *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
	var resp model.InitiateFormDataResp
	if err := m.http.Post(ctx, "/third/formdata/init", req, &resp); err != nil {
		return nil, fmt.Errorf("third InitiateFormData: %w", err)
	}
	return &resp, nil
}

// CompleteFormData FormData 上传完成
func (m *DefaultThirdMgr) CompleteFormData(ctx context.Context, req *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error) {
	var resp model.CompleteFormDataResp
	if err := m.http.Post(ctx, "/third/formdata/complete", req, &resp); err != nil {
		return nil, fmt.Errorf("third CompleteFormData: %w", err)
	}
	return &resp, nil
}

// DeleteOutdatedData 清理过期数据（管理接口）
func (m *DefaultThirdMgr) DeleteOutdatedData(ctx context.Context, req *model.DeleteOutdatedDataReq) (*model.DeleteOutdatedDataResp, error) {
	var resp model.DeleteOutdatedDataResp
	if err := m.http.Post(ctx, "/third/delete_outdated", req, &resp); err != nil {
		return nil, fmt.Errorf("third DeleteOutdatedData: %w", err)
	}
	return &resp, nil
}

// urlValuesFromMap 把 map[string]string 转成 url.Values（AccessURL GET query 用）
func urlValuesFromMap(m map[string]string) url.Values {
	out := url.Values{}
	for k, v := range m {
		out.Set(k, v)
	}
	return out
}
