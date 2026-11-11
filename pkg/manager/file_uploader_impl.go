package manager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/parallel"
)

// DefaultFileUploader FileUploader 默认实现
//
// 设计要点：
//   - 组合 ThirdMgr 作为签名控制层（init/sign/complete/abort）
//   - 文件字节传输用标准库 net/http PUT 到预签名 URL（不需要第三方 S3 SDK）
//   - Multipart 路径并发度默认 4，单片失败重试 3 次
//   - ctx 取消时终止并发 PUT，**保留服务端已上传分片**（不调 AbortMultipartUpload），
//     返回的 UploadResult 带 ResumeState 供调用方持久化断点续传
//   - 显式放弃调 Abort() 或 Third().AbortMultipartUpload()
type DefaultFileUploader struct {
	third  ThirdMgr
	logger logger.Logger

	// HTTPClient 用于 PUT 预签名 URL / FormData POST（超时、连接池、Transport 可自定义）
	// 设 nil 时用默认 http.Client（DefaultHTTPPutTimeout 超时）
	httpClient *http.Client
}

// NewFileUploader 构造 FileUploader
func NewFileUploader(third ThirdMgr, l logger.Logger) *DefaultFileUploader {
	return NewFileUploaderWithClient(third, l, nil)
}

// NewFileUploaderWithClient 构造 FileUploader（自定义 HTTP 客户端，如带代理/超时）
func NewFileUploaderWithClient(third ThirdMgr, l logger.Logger, hc *http.Client) *DefaultFileUploader {
	if hc == nil {
		hc = &http.Client{Timeout: DefaultHTTPPutTimeout}
	}
	return &DefaultFileUploader{third: third, logger: l, httpClient: hc}
}

// Upload 一站式上传：自动选择路径 → 传输 → 完成通知 → 返回最终 URL
func (u *DefaultFileUploader) Upload(ctx context.Context, reader io.Reader, opts *UploadOptions) (*UploadResult, error) {
	if opts == nil || opts.Name == "" || opts.Size <= 0 {
		return nil, fmt.Errorf("Upload: opts.Name/Size 必填")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultUploadConcurrency
	}
	if opts.PartRetry <= 0 {
		opts.PartRetry = DefaultPartRetry
	}

	// 断点续传强制走 Multipart
	if opts.Resume != nil {
		opts.ForceMultipart = true
	}

	// 路径选择
	var multipart bool
	switch {
	case opts.ForceMultipart:
		multipart = true
	case opts.ForceFormData:
		multipart = false
	default:
		multipart = opts.Size >= DefaultMultipartThreshold
	}

	if multipart {
		return u.uploadMultipart(ctx, reader, opts)
	}
	return u.uploadFormData(ctx, reader, opts)
}

// Abort 彻底放弃 Multipart 上传（清理服务端已上传分片），不可续传
func (u *DefaultFileUploader) Abort(ctx context.Context, uploadID, name, urlPrefix string) error {
	return u.third.AbortMultipartUpload(ctx, &model.AbortMultipartUploadReq{
		UploadID:  uploadID,
		Name:      name,
		URLPrefix: urlPrefix,
	})
}

// —— FormData 小文件直传 ——

func (u *DefaultFileUploader) uploadFormData(ctx context.Context, reader io.Reader, opts *UploadOptions) (*UploadResult, error) {
	// ① 初始化：拿预签名表单字段
	initResp, err := u.third.InitiateFormData(ctx, &model.InitiateFormDataReq{
		Name:        opts.Name,
		Size:        opts.Size,
		ContentType: opts.ContentType,
	})
	if err != nil {
		return nil, fmt.Errorf("InitiateFormData: %w", err)
	}

	// u.logger.Infof("InitiateFormData: %+v", initResp)

	// ② 构造 multipart/form-data 请求（预签名字段 + 文件内容）
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	// 预签名字段全部写入 formData
	for k, v := range initResp.FormData {
		if err := w.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("write form field %s: %w", k, err)
		}
	}

	// 写文件内容（S3 预签名 POST 规定文件分片字段名必须为 "file"，
	// 且必须是最后一个字段；initResp.File 是对象 key，不作为字段名使用）
	part, err := w.CreateFormFile("file", opts.Name)
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, reader); err != nil {
		return nil, fmt.Errorf("copy file to form: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close form writer: %w", err)
	}

	// ③ POST 到预签名 URL（带 header）
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, initResp.URL, &body)
	if err != nil {
		return nil, fmt.Errorf("build FormData POST: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	// 预签名 header 注入
	for _, kv := range initResp.Header {
		if len(kv.Values) > 0 {
			req.Header.Set(kv.Key, kv.Values[0])
		}
	}

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("FormData POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("FormData POST status %d, body: %s", resp.StatusCode, string(body))
	}

	// ④ 完成通知（服务端最终生成可访问 URL）
	completeResp, err := u.third.CompleteFormData(ctx, &model.CompleteFormDataReq{
		ID:        initResp.ID,
		URLPrefix: opts.URLPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("CompleteFormData: %w", err)
	}

	return &UploadResult{URL: completeResp.URL, Multipart: false}, nil
}

// —— Multipart 分片上传（支持断点续传） ——

func (u *DefaultFileUploader) uploadMultipart(ctx context.Context, reader io.Reader, opts *UploadOptions) (*UploadResult, error) {
	// ① PartSize + PartLimit（续传时必须和原来一致）
	partSize := opts.PartSize
	if opts.Resume != nil {
		partSize = opts.Resume.PartSize
	}
	if partSize <= 0 {
		ps, err := u.third.PartSize(ctx, opts.Size)
		if err != nil {
			return nil, fmt.Errorf("PartSize: %w", err)
		}
		partSize = ps.Size
	}
	var maxParts int32
	if opts.MaxParts > 0 {
		maxParts = opts.MaxParts
	} else {
		limit, err := u.third.PartLimit(ctx)
		if err != nil {
			return nil, fmt.Errorf("PartLimit: %w", err)
		}
		maxParts = limit.MaxNumSize
	}
	numParts := int32((opts.Size + partSize - 1) / partSize)
	if numParts > maxParts {
		return nil, fmt.Errorf("file requires %d parts, exceeds server limit %d", numParts, maxParts)
	}

	// ② 把文件读进内存
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read file data: %w", err)
	}
	dataLen := int64(len(data))

	// ③ 确定 uploadID + 已完成分片（新上传 vs 续传）
	var uploadID string
	var completedParts []model.CompletedPart
	var initSigned []model.SignPart // 新上传时 Initiate 已附带的预签名，直接复用避免重复签名
	if opts.Resume != nil {
		// 续传：跳过 Initiate，用已有 uploadID
		uploadID = opts.Resume.UploadID
		completedParts = opts.Resume.CompletedParts
	} else {
		// 新上传：InitiateMultipartUpload
		initResp, initErr := u.third.InitiateMultipartUpload(ctx, &model.InitiateMultipartUploadReq{
			Hash:        opts.Hash,
			Size:        opts.Size,
			PartSize:    partSize,
			MaxParts:    numParts,
			Cause:       opts.Cause,
			Name:        opts.Name,
			ContentType: opts.ContentType,
			URLPrefix:   opts.URLPrefix,
		})
		if initErr != nil {
			return nil, fmt.Errorf("InitiateMultipartUpload: %w", initErr)
		}
		uploadID = initResp.Upload.UploadID
		initSigned = initResp.Upload.Sign.Parts
	}

	// 建立已完成分片索引（续传时跳过这些 PUT）
	doneMap := make(map[int32]string, len(completedParts))
	for _, cp := range completedParts {
		doneMap[cp.PartNumber] = cp.ETag
	}

	// 准备最终 CompleteMultipartUpload 所需的全部分片列表（续传时要把已完成的也带上）
	// 后续 PUT 成功后会填 ETag
	allCompleted := make([]model.CompletedPart, numParts)
	for _, cp := range completedParts {
		if cp.PartNumber >= 1 && cp.PartNumber <= numParts {
			allCompleted[cp.PartNumber-1] = cp
		}
	}

	// ④ 复用 Initiate 已返回的预签名；只有缺失分片才调 AuthSign（最多 1 次）
	type signInfo struct {
		url    string
		header []model.KeyValues
		query  []model.KeyValues
	}
	signs := make(map[int32]signInfo, numParts)
	for _, sp := range initSigned {
		signs[sp.PartNumber] = signInfo{url: sp.URL, header: sp.Header, query: sp.Query}
	}

	// needSign 只挑「未完成 且 Initiate 未签」的分片补签；补完后 signs 即全部待上传分片
	needSign := make([]int32, 0, numParts)
	for i := int32(1); i <= numParts; i++ {
		if _, done := doneMap[i]; done {
			continue // 续传已完成，跳过
		}
		if _, ok := signs[i]; !ok {
			needSign = append(needSign, i)
		}
	}

	// partial 中断时返回当前进度（含 ResumeState，调用方持久化即可续传）
	partial := func(err error) (*UploadResult, error) {
		parts := append([]model.CompletedPart(nil), completedParts...)
		return &UploadResult{
			Multipart:      true,
			UploadID:       uploadID,
			PartSize:       partSize,
			CompletedParts: parts,
			ResumeState: &ResumeState{
				UploadID:       uploadID,
				PartSize:       partSize,
				CompletedParts: parts,
			},
		}, err
	}

	if len(needSign) > 0 {
		signed, signErr := u.third.AuthSign(ctx, &model.AuthSignReq{
			UploadID:    uploadID,
			PartNumbers: needSign,
			Name:        opts.Name,
			URLPrefix:   opts.URLPrefix,
		})
		if signErr != nil {
			// 新上传签名失败才清理；续传保留现场供下次续传
			if opts.Resume == nil {
				_ = u.Abort(context.Background(), uploadID, opts.Name, opts.URLPrefix)
			}
			return partial(fmt.Errorf("AuthSign: %w", signErr))
		}
		for _, sp := range signed.Parts {
			signs[sp.PartNumber] = signInfo{url: sp.URL, header: sp.Header, query: sp.Query}
		}
	}

	// ⑤ 构造待 PUT 任务：signs = Initiate 预签名 ∪ AuthSign 补签 = 全部待上传分片
	type partTask struct {
		partNum int32
		data    []byte
		url     string
		header  []model.KeyValues
		query   []model.KeyValues
	}
	partTasks := make([]partTask, 0, len(signs))
	for pn, sign := range signs {
		startOffset := int64(pn-1) * partSize
		if pn < 1 || startOffset >= dataLen {
			return partial(fmt.Errorf("invalid part number %d from signer", pn))
		}
		endOffset := startOffset + partSize
		if endOffset > dataLen {
			endOffset = dataLen
		}
		pd := data[startOffset:endOffset]
		partTasks = append(partTasks, partTask{pn, pd, sign.url, sign.header, sign.query})
	}

	// ⑥ 并发 PUT（任何错误——包括 ctx cancel——都返回当前进度，不 abort 服务端）
	var uploadedBytes int64
	putErr := parallel.Run(partTasks, opts.Concurrency, func(pt partTask) error {
		etag, err := u.putPart(ctx, pt.partNum, pt.data, pt.url, pt.header, pt.query, opts.PartRetry)
		if err != nil {
			return fmt.Errorf("put part %d: %w", pt.partNum, err)
		}
		allCompleted[pt.partNum-1] = model.CompletedPart{PartNumber: pt.partNum, ETag: etag}
		added := atomic.AddInt64(&uploadedBytes, int64(len(pt.data)))
		if opts.OnProgress != nil {
			opts.OnProgress(added+int64(len(completedParts))*partSize, opts.Size)
		}
		return nil
	})

	// 收集本次 PUT 成功的分片
	var nowDone []model.CompletedPart
	for _, cp := range allCompleted {
		if cp.ETag != "" {
			nowDone = append(nowDone, cp)
		}
	}

	// 构造 ResumeState（不管成功失败都返回，让调用方持久化）
	resumeState := &ResumeState{
		UploadID:       uploadID,
		PartSize:       partSize,
		CompletedParts: nowDone,
	}
	result := &UploadResult{
		Multipart:      true,
		UploadID:       uploadID,
		PartSize:       partSize,
		CompletedParts: nowDone,
		ResumeState:    resumeState,
	}

	if putErr != nil {
		// 上传中断：ctx cancel 或分片失败
		// 服务端分片保留，供调用方续传或 Abort
		if ctx.Err() != nil {
			// ctx cancel：正常中断，返回进度让调用方持久化
			u.logger.Infof("upload interrupted (ctx=%v), uploadID=%s, %d/%d parts done",
				ctx.Err(), uploadID, len(nowDone), numParts)
			return result, ctx.Err()
		}
		// 分片 PUT 非 ctx 错误：同样保留现场供续传
		u.logger.Warnf("upload failed, uploadID=%s, %d/%d parts done, err=%v",
			uploadID, len(nowDone), numParts, putErr)
		return result, putErr
	}

	// ⑦ 全部 PUT 成功 → CompleteMultipartUpload（服务端合并分片 → 最终 URL）
	// Complete 本身支持 ctx cancel——如果合并超时，分片已 PUT 成功，下次续传要从 Complete 恢复
	completeResp, err := u.third.CompleteMultipartUpload(ctx, &model.CompleteMultipartUploadReq{
		UploadID:    uploadID,
		Parts:       nowDone,
		Name:        opts.Name,
		ContentType: opts.ContentType,
		Cause:       opts.Cause,
		URLPrefix:   opts.URLPrefix,
	})
	if err != nil {
		// Complete 失败：分片已上传成功，但服务端未合并。
		// S3/RustFS 会保留已上传分片，下次续传可重新 Complete
		u.logger.Warnf("CompleteMultipartUpload failed, uploadID=%s, err=%v (parts kept for resume)",
			uploadID, err)
		return result, fmt.Errorf("CompleteMultipartUpload: %w", err)
	}

	result.URL = completeResp.URL
	result.ResumeState = nil // 上传成功，不需要续传状态
	return result, nil
}

// putPart PUT 单个分片到预签名 URL（带 header 注入 + 重试 + ETag 提取）
func (u *DefaultFileUploader) putPart(ctx context.Context, pn int32, data []byte, url string, header, query []model.KeyValues, retries int) (string, error) {
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			// 指数退避
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
		if err != nil {
			lastErr = err
			continue
		}
		req.ContentLength = int64(len(data))
		// S3 预签名需要指定 Content-Length（避免 chunked transfer）
		req.Header.Set("Content-Type", "application/octet-stream")
		for _, kv := range header {
			if len(kv.Values) > 0 {
				req.Header.Set(kv.Key, kv.Values[0])
			}
		}

		resp, err := u.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		// S3 PUT 成功返回 200 OK，ETag 在响应头
		if resp.StatusCode == http.StatusOK {
			etag := resp.Header.Get("ETag")
			resp.Body.Close()
			if etag == "" {
				// 有些代理/网关剥了 ETag——从完整响应（含引号）兜底取
				etag = `"` + fmt.Sprintf("%x", data) + `"`
				u.logger.Warnf("part %d missing ETag header, fallback to sha-like etag", pn)
			}
			return etag, nil
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastErr = fmt.Errorf("PUT part %d status %d: %s", pn, resp.StatusCode, string(body[:min(len(body), 512)]))
	}
	return "", lastErr
}
