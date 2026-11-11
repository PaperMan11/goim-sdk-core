package manager

// FileUploader 单测计划
//
// 覆盖点：
//  1. 参数校验：opts 为空 / Name 缺失 / Size<=0 直接报错
//  2. 路径选择：小文件走 FormData、大文件走 Multipart、ForceXxx 覆盖、Resume 强制 Multipart
//  3. FormData：初始化 → 表单 POST（预签名字段 + 文件 + header 注入）→ 完成通知
//       以及初始化失败 / POST 非 2xx / 完成失败三条错误链路
//  4. Multipart：复用 Initiate 预签名（不重复 AuthSign）、并发 PUT 切片正确、Complete 分片齐全
//  5. Multipart 补签：Initiate 缺签的分片才调 AuthSign，顺序升序
//  6. Multipart 约束：分片数超过服务端上限时提前报错（不发起 Initiate）
//  7. 断点续传：跳过已完成分片、不再 Initiate、Complete 带上历史分片
//  8. 中断/失败：分片 PUT 失败与 ctx cancel 都保留现场（ResumeState 非空，不 Abort）
//  9. Complete 失败：分片保留，结果仍带 ResumeState
// 10. ETag 缺失兜底：PUT 200 但无 ETag 时生成占位 ETag
// 11. Abort：正确透传 uploadID/name/urlPrefix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
)

// stubThirdMgr 可注入函数字段的 ThirdMgr 假实现；未注入的方法视为非预期调用
type stubThirdMgr struct {
	partLimitFn         func(ctx context.Context) (*model.PartLimitResp, error)
	partSizeFn          func(ctx context.Context, size int64) (*model.PartSizeResp, error)
	initiateMultipartFn func(ctx context.Context, req *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error)
	authSignFn          func(ctx context.Context, req *model.AuthSignReq) (*model.AuthSignResp, error)
	completeMultipartFn func(ctx context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error)
	abortMultipartFn    func(ctx context.Context, req *model.AbortMultipartUploadReq) error
	accessURLFn         func(ctx context.Context, req *model.AccessURLReq) (*model.AccessURLResp, error)
	initiateFormDataFn  func(ctx context.Context, req *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error)
	completeFormDataFn  func(ctx context.Context, req *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error)
	deleteOutdatedFn    func(ctx context.Context, req *model.DeleteOutdatedDataReq) (*model.DeleteOutdatedDataResp, error)
}

func unexpectedCall(name string) error { return fmt.Errorf("unexpected stub call: %s", name) }

func (s *stubThirdMgr) PartLimit(ctx context.Context) (*model.PartLimitResp, error) {
	if s.partLimitFn != nil {
		return s.partLimitFn(ctx)
	}
	return nil, unexpectedCall("PartLimit")
}

func (s *stubThirdMgr) PartSize(ctx context.Context, size int64) (*model.PartSizeResp, error) {
	if s.partSizeFn != nil {
		return s.partSizeFn(ctx, size)
	}
	return nil, unexpectedCall("PartSize")
}

func (s *stubThirdMgr) InitiateMultipartUpload(ctx context.Context, req *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
	if s.initiateMultipartFn != nil {
		return s.initiateMultipartFn(ctx, req)
	}
	return nil, unexpectedCall("InitiateMultipartUpload")
}

func (s *stubThirdMgr) AuthSign(ctx context.Context, req *model.AuthSignReq) (*model.AuthSignResp, error) {
	if s.authSignFn != nil {
		return s.authSignFn(ctx, req)
	}
	return nil, unexpectedCall("AuthSign")
}

func (s *stubThirdMgr) CompleteMultipartUpload(ctx context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
	if s.completeMultipartFn != nil {
		return s.completeMultipartFn(ctx, req)
	}
	return nil, unexpectedCall("CompleteMultipartUpload")
}

func (s *stubThirdMgr) AbortMultipartUpload(ctx context.Context, req *model.AbortMultipartUploadReq) error {
	if s.abortMultipartFn != nil {
		return s.abortMultipartFn(ctx, req)
	}
	return unexpectedCall("AbortMultipartUpload")
}

func (s *stubThirdMgr) AccessURL(ctx context.Context, req *model.AccessURLReq) (*model.AccessURLResp, error) {
	if s.accessURLFn != nil {
		return s.accessURLFn(ctx, req)
	}
	return nil, unexpectedCall("AccessURL")
}

func (s *stubThirdMgr) InitiateFormData(ctx context.Context, req *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
	if s.initiateFormDataFn != nil {
		return s.initiateFormDataFn(ctx, req)
	}
	return nil, unexpectedCall("InitiateFormData")
}

func (s *stubThirdMgr) CompleteFormData(ctx context.Context, req *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error) {
	if s.completeFormDataFn != nil {
		return s.completeFormDataFn(ctx, req)
	}
	return nil, unexpectedCall("CompleteFormData")
}

func (s *stubThirdMgr) DeleteOutdatedData(ctx context.Context, req *model.DeleteOutdatedDataReq) (*model.DeleteOutdatedDataResp, error) {
	if s.deleteOutdatedFn != nil {
		return s.deleteOutdatedFn(ctx, req)
	}
	return nil, unexpectedCall("DeleteOutdatedData")
}

// —— 测试辅助 ——

// putRecord 记录各分片 PUT 请求
type putRecord struct {
	mu      sync.Mutex
	bodies  map[int32][]byte
	headers map[int32]http.Header
	methods map[int32]string
}

func (r *putRecord) get(pn int32) ([]byte, http.Header, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	body, ok := r.bodies[pn]
	if !ok {
		return nil, nil, "", false
	}
	return body, r.headers[pn], r.methods[pn], true
}

func (r *putRecord) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

// newPutServer 预签名 PUT 目标：按路径 /part/{n} 记录请求并返回 ETag
func newPutServer(t *testing.T, withETag bool) (*httptest.Server, *putRecord) {
	t.Helper()
	rec := &putRecord{
		bodies:  map[int32][]byte{},
		headers: map[int32]http.Header{},
		methods: map[int32]string{},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pn, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/part/"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.bodies[int32(pn)] = body
		rec.headers[int32(pn)] = r.Header.Clone()
		rec.methods[int32(pn)] = r.Method
		rec.mu.Unlock()

		if withETag {
			w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, pn))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// signsFor 构造分片预签名列表（URL 指向测试服务器 /part/{n}）
func signsFor(srvURL string, nums ...int32) []model.SignPart {
	parts := make([]model.SignPart, 0, len(nums))
	for _, n := range nums {
		parts = append(parts, model.SignPart{
			PartNumber: n,
			URL:        fmt.Sprintf("%s/part/%d", srvURL, n),
		})
	}
	return parts
}

func newTestUploader(third ThirdMgr) *DefaultFileUploader {
	return NewFileUploader(third, &logger.NopLogger{})
}

// —— 1. 参数校验 ——

func TestUploadInvalidOptions(t *testing.T) {
	u := newTestUploader(&stubThirdMgr{})
	cases := []*UploadOptions{
		nil,
		{},
		{Name: "a.bin"},
		{Name: "a.bin", Size: -1},
	}
	for _, opts := range cases {
		_, err := u.Upload(context.Background(), strings.NewReader("x"), opts)
		require.Error(t, err)
	}
}

// —— 2/3. FormData 链路 ——

func TestUploadFormDataSuccess(t *testing.T) {
	var gotBody []byte
	var gotHeader http.Header
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	var initReq *model.InitiateFormDataReq
	var completeReq *model.CompleteFormDataReq
	third := &stubThirdMgr{
		initiateFormDataFn: func(_ context.Context, req *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
			initReq = req
			return &model.InitiateFormDataResp{
				ID:       "id-1",
				URL:      srv.URL + "/formdata",
				File:     "it-upload/a.jpg", // 对象 key，不应被当作表单字段名
				Header:   []model.KeyValues{{Key: "X-Sign", Values: []string{"sig-value"}}},
				FormData: map[string]string{"policy": "p-1", "key": "photos/a.jpg"},
			}, nil
		},
		completeFormDataFn: func(_ context.Context, req *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error) {
			completeReq = req
			return &model.CompleteFormDataResp{URL: "https://cdn.example.com/photos/a.jpg"}, nil
		},
	}

	res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("hello"), &UploadOptions{
		Name:        "photos/a.jpg",
		Size:        5,
		ContentType: "image/jpeg",
		URLPrefix:   "https://cdn.example.com",
	})
	require.NoError(t, err)

	// 初始化请求参数
	require.NotNil(t, initReq)
	require.Equal(t, "photos/a.jpg", initReq.Name)
	require.Equal(t, int64(5), initReq.Size)
	require.Equal(t, "image/jpeg", initReq.ContentType)

	// 表单 POST：方法 / 预签名 header / 表单字段 / 文件内容
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "sig-value", gotHeader.Get("X-Sign"))
	require.Contains(t, gotHeader.Get("Content-Type"), "multipart/form-data")
	body := string(gotBody)
	require.Contains(t, body, `name="policy"`)
	require.Contains(t, body, "p-1")
	// S3 规定文件分片字段名必须为 "file"，而非 initResp.File（对象 key）
	require.Contains(t, body, `name="file"; filename="photos/a.jpg"`)
	require.Contains(t, body, "hello")

	// 完成通知 + 结果
	require.NotNil(t, completeReq)
	require.Equal(t, "id-1", completeReq.ID)
	require.Equal(t, "https://cdn.example.com", completeReq.URLPrefix)
	require.False(t, res.Multipart)
	require.Equal(t, "https://cdn.example.com/photos/a.jpg", res.URL)
	require.Nil(t, res.ResumeState)
}

func TestUploadFormDataErrors(t *testing.T) {
	t.Run("initiate error", func(t *testing.T) {
		third := &stubThirdMgr{
			initiateFormDataFn: func(context.Context, *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
				return nil, errors.New("init boom")
			},
		}
		_, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("x"), &UploadOptions{Name: "a", Size: 1})
		require.ErrorContains(t, err, "InitiateFormData")
		require.ErrorContains(t, err, "init boom")
	})

	t.Run("post non-2xx", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(srv.Close)
		completeCalled := false
		third := &stubThirdMgr{
			initiateFormDataFn: func(context.Context, *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
				return &model.InitiateFormDataResp{ID: "id", URL: srv.URL + "/form", File: "file"}, nil
			},
			completeFormDataFn: func(context.Context, *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error) {
				completeCalled = true
				return &model.CompleteFormDataResp{URL: "u"}, nil
			},
		}
		_, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("x"), &UploadOptions{Name: "a", Size: 1})
		require.ErrorContains(t, err, "status 500")
		require.False(t, completeCalled, "POST 失败不应调用 CompleteFormData")
	})

	t.Run("complete error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(srv.Close)
		third := &stubThirdMgr{
			initiateFormDataFn: func(context.Context, *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
				return &model.InitiateFormDataResp{ID: "id", URL: srv.URL + "/form", File: "file"}, nil
			},
			completeFormDataFn: func(context.Context, *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error) {
				return nil, errors.New("complete boom")
			},
		}
		_, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("x"), &UploadOptions{Name: "a", Size: 1})
		require.ErrorContains(t, err, "CompleteFormData")
		require.ErrorContains(t, err, "complete boom")
	})
}

// —— 2. 路径选择 ——

func TestUploadAutoPathSelection(t *testing.T) {
	t.Run("size >= threshold uses multipart", func(t *testing.T) {
		srv, _ := newPutServer(t, true)
		initCalled := false
		third := &stubThirdMgr{
			initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
				initCalled = true
				return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
					UploadID: "up",
					Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1)},
				}}, nil
			},
			completeMultipartFn: func(context.Context, *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
				return &model.CompleteMultipartUploadResp{URL: "https://cdn/a.bin"}, nil
			},
		}
		res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("12345678"), &UploadOptions{
			Name:     "a.bin",
			Size:     DefaultMultipartThreshold,
			PartSize: DefaultMultipartThreshold, // 单片，避免读 8MB 数据
			MaxParts: 10,
		})
		require.NoError(t, err)
		require.True(t, initCalled)
		require.True(t, res.Multipart)
	})

	t.Run("size < threshold uses formdata", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(srv.Close)
		formCalled := false
		third := &stubThirdMgr{
			initiateFormDataFn: func(context.Context, *model.InitiateFormDataReq) (*model.InitiateFormDataResp, error) {
				formCalled = true
				return &model.InitiateFormDataResp{ID: "id", URL: srv.URL + "/form", File: "file"}, nil
			},
			completeFormDataFn: func(context.Context, *model.CompleteFormDataReq) (*model.CompleteFormDataResp, error) {
				return &model.CompleteFormDataResp{URL: "https://cdn/a.bin"}, nil
			},
		}
		res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("1234567"), &UploadOptions{
			Name: "a.bin",
			Size: DefaultMultipartThreshold - 1,
		})
		require.NoError(t, err)
		require.True(t, formCalled)
		require.False(t, res.Multipart)
	})
}

// —— 4. Multipart 成功链路 ——

func TestUploadMultipartSuccessReusesInitiateSigns(t *testing.T) {
	srv, rec := newPutServer(t, true)

	var initReq *model.InitiateMultipartUploadReq
	var completeReq *model.CompleteMultipartUploadReq
	authCalled := false
	third := &stubThirdMgr{
		initiateMultipartFn: func(_ context.Context, req *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			initReq = req
			return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
				UploadID: "up-1",
				PartSize: 4,
				Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1, 2, 3)},
			}}, nil
		},
		authSignFn: func(context.Context, *model.AuthSignReq) (*model.AuthSignResp, error) {
			authCalled = true
			return nil, errors.New("AuthSign should not be called")
		},
		completeMultipartFn: func(_ context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
			completeReq = req
			return &model.CompleteMultipartUploadResp{URL: "https://cdn.example.com/a.bin"}, nil
		},
	}

	var mu sync.Mutex
	var progressTotals []int64
	var lastUploaded int64
	res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123456789"), &UploadOptions{
		Name:           "a.bin",
		Size:           10,
		ForceMultipart: true,
		PartSize:       4,
		MaxParts:       8,
		Concurrency:    3,
		OnProgress: func(uploaded, total int64) {
			mu.Lock()
			progressTotals = append(progressTotals, total)
			if uploaded > lastUploaded {
				lastUploaded = uploaded
			}
			mu.Unlock()
		},
	})
	require.NoError(t, err)

	// Initiate 参数：按 Size/PartSize 推导出 3 个分片
	require.NotNil(t, initReq)
	require.Equal(t, int64(10), initReq.Size)
	require.Equal(t, int64(4), initReq.PartSize)
	require.Equal(t, int32(3), initReq.MaxParts)
	require.Equal(t, "a.bin", initReq.Name)

	// Initiate 已给全量签名，不应再调 AuthSign
	require.False(t, authCalled)

	// 分片字节切片正确，且全部走 PUT
	for pn, want := range map[int32]string{1: "0123", 2: "4567", 3: "89"} {
		body, _, method, ok := rec.get(pn)
		require.True(t, ok, "part %d not uploaded", pn)
		require.Equal(t, http.MethodPut, method)
		require.Equal(t, want, string(body), "part %d body", pn)
	}
	require.Equal(t, 3, rec.count())

	// Complete 分片齐全且有序
	require.NotNil(t, completeReq)
	require.Equal(t, "up-1", completeReq.UploadID)
	require.Equal(t, []model.CompletedPart{
		{PartNumber: 1, ETag: `"etag-1"`},
		{PartNumber: 2, ETag: `"etag-2"`},
		{PartNumber: 3, ETag: `"etag-3"`},
	}, completeReq.Parts)

	// 结果
	require.True(t, res.Multipart)
	require.Equal(t, "https://cdn.example.com/a.bin", res.URL)
	require.Equal(t, "up-1", res.UploadID)
	require.Equal(t, int64(4), res.PartSize)
	require.Len(t, res.CompletedParts, 3)
	require.Nil(t, res.ResumeState, "成功上传不应返回续传状态")

	// 进度：总字节数正确，最终已上传 = 10
	mu.Lock()
	defer mu.Unlock()
	for _, total := range progressTotals {
		require.Equal(t, int64(10), total)
	}
	require.Equal(t, int64(10), lastUploaded)
}

// —— 5. Multipart 补签 ——

func TestUploadMultipartAuthSignForMissingParts(t *testing.T) {
	srv, rec := newPutServer(t, true)

	var authReq *model.AuthSignReq
	var completeReq *model.CompleteMultipartUploadReq
	third := &stubThirdMgr{
		initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			// 只给了 part 1 的预签名，2/3 需要补签
			return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
				UploadID: "up-2",
				Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1)},
			}}, nil
		},
		authSignFn: func(_ context.Context, req *model.AuthSignReq) (*model.AuthSignResp, error) {
			authReq = req
			return &model.AuthSignResp{Parts: signsFor(srv.URL, 2, 3)}, nil
		},
		completeMultipartFn: func(_ context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
			completeReq = req
			return &model.CompleteMultipartUploadResp{URL: "https://cdn/a.bin"}, nil
		},
	}

	res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123456789"), &UploadOptions{
		Name: "a.bin", Size: 10, ForceMultipart: true, PartSize: 4, MaxParts: 8,
	})
	require.NoError(t, err)
	require.NotNil(t, authReq)
	require.Equal(t, "up-2", authReq.UploadID)
	require.Equal(t, []int32{2, 3}, authReq.PartNumbers)
	require.Equal(t, 3, rec.count())
	require.Len(t, completeReq.Parts, 3)
	require.Equal(t, "https://cdn/a.bin", res.URL)
}

// —— 6. 分片数超限 ——

func TestUploadMultipartExceedsMaxParts(t *testing.T) {
	third := &stubThirdMgr{} // 任何调用都会返回 unexpected 错误
	_, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123456789"), &UploadOptions{
		Name: "a.bin", Size: 10, ForceMultipart: true, PartSize: 4, MaxParts: 2,
	})
	require.ErrorContains(t, err, "exceeds server limit")
}

// —— 7. 断点续传 ——

func TestUploadMultipartResumeSkipsCompletedParts(t *testing.T) {
	srv, rec := newPutServer(t, true)

	initCalled := false
	var authReq *model.AuthSignReq
	var completeReq *model.CompleteMultipartUploadReq
	third := &stubThirdMgr{
		initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			initCalled = true
			return nil, errors.New("InitiateMultipartUpload should not be called on resume")
		},
		authSignFn: func(_ context.Context, req *model.AuthSignReq) (*model.AuthSignResp, error) {
			authReq = req
			return &model.AuthSignResp{Parts: signsFor(srv.URL, 3)}, nil
		},
		completeMultipartFn: func(_ context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
			completeReq = req
			return &model.CompleteMultipartUploadResp{URL: "https://cdn/a.bin"}, nil
		},
	}

	res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123456789"), &UploadOptions{
		Name:     "a.bin",
		Size:     10,
		MaxParts: 8,
		Resume: &ResumeState{
			UploadID: "up-r",
			PartSize: 4,
			CompletedParts: []model.CompletedPart{
				{PartNumber: 1, ETag: `"e1"`},
				{PartNumber: 2, ETag: `"e2"`},
			},
		},
	})
	require.NoError(t, err)

	// 续传不再 Initiate，只补签/补传 part 3
	require.False(t, initCalled)
	require.NotNil(t, authReq)
	require.Equal(t, []int32{3}, authReq.PartNumbers)
	require.Equal(t, 1, rec.count())
	_, _, _, ok := rec.get(3)
	require.True(t, ok)
	if _, _, _, ok := rec.get(1); ok {
		t.Fatal("已完成分片不应重复 PUT")
	}

	// Complete 必须带上历史分片
	require.NotNil(t, completeReq)
	require.Equal(t, "up-r", completeReq.UploadID)
	require.Equal(t, []model.CompletedPart{
		{PartNumber: 1, ETag: `"e1"`},
		{PartNumber: 2, ETag: `"e2"`},
		{PartNumber: 3, ETag: `"etag-3"`},
	}, completeReq.Parts)

	require.Equal(t, "up-r", res.UploadID)
	require.Equal(t, "https://cdn/a.bin", res.URL)
	require.Nil(t, res.ResumeState, "续传成功后应清除续传状态")
}

// —— 8. 中断/失败保留现场 ——

func TestUploadMultipartPartFailureKeepsResumeState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	completeCalled := false
	third := &stubThirdMgr{
		initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
				UploadID: "up-f",
				Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1, 2, 3)},
			}}, nil
		},
		completeMultipartFn: func(context.Context, *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
			completeCalled = true
			return &model.CompleteMultipartUploadResp{URL: "u"}, nil
		},
	}

	res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123456789"), &UploadOptions{
		Name: "a.bin", Size: 10, ForceMultipart: true, PartSize: 4, MaxParts: 8,
		Concurrency: 3, PartRetry: 1,
	})
	require.ErrorContains(t, err, "put part")
	require.False(t, completeCalled, "分片失败不应调用 Complete")
	require.NotNil(t, res)
	require.NotNil(t, res.ResumeState, "失败时应返回续传现场")
	require.Equal(t, "up-f", res.ResumeState.UploadID)
	require.Equal(t, int64(4), res.ResumeState.PartSize)
	require.Empty(t, res.CompletedParts)
	require.Empty(t, res.URL)
}

func TestUploadMultipartCtxCancelKeepsResumeState(t *testing.T) {
	srv, _ := newPutServer(t, true)

	third := &stubThirdMgr{
		initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
				UploadID: "up-c",
				Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1, 2, 3)},
			}}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := newTestUploader(third).Upload(ctx, strings.NewReader("0123456789"), &UploadOptions{
		Name: "a.bin", Size: 10, ForceMultipart: true, PartSize: 4, MaxParts: 8,
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, res)
	require.NotNil(t, res.ResumeState)
	require.Equal(t, "up-c", res.ResumeState.UploadID)
	require.Empty(t, res.CompletedParts)
}

// —— 9. Complete 失败 ——

func TestUploadMultipartCompleteFailureKeepsParts(t *testing.T) {
	srv, _ := newPutServer(t, true)

	third := &stubThirdMgr{
		initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
				UploadID: "up-x",
				Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1, 2, 3)},
			}}, nil
		},
		completeMultipartFn: func(context.Context, *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
			return nil, errors.New("merge boom")
		},
	}

	res, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123456789"), &UploadOptions{
		Name: "a.bin", Size: 10, ForceMultipart: true, PartSize: 4, MaxParts: 8,
	})
	require.ErrorContains(t, err, "CompleteMultipartUpload")
	require.ErrorContains(t, err, "merge boom")
	require.NotNil(t, res)
	require.Empty(t, res.URL, "Complete 失败时不应返回可用 URL")
	require.Len(t, res.CompletedParts, 3, "已 PUT 成功的分片应保留")
	require.NotNil(t, res.ResumeState)
	require.Equal(t, "up-x", res.ResumeState.UploadID)
}

// —— 10. ETag 缺失兜底 ——

func TestUploadMultipartMissingETagFallback(t *testing.T) {
	srv, _ := newPutServer(t, false) // 不返回 ETag

	var completeReq *model.CompleteMultipartUploadReq
	third := &stubThirdMgr{
		initiateMultipartFn: func(context.Context, *model.InitiateMultipartUploadReq) (*model.InitiateMultipartUploadResp, error) {
			return &model.InitiateMultipartUploadResp{Upload: model.UploadInfo{
				UploadID: "up-e",
				Sign:     model.AuthSignParts{Parts: signsFor(srv.URL, 1)},
			}}, nil
		},
		completeMultipartFn: func(_ context.Context, req *model.CompleteMultipartUploadReq) (*model.CompleteMultipartUploadResp, error) {
			completeReq = req
			return &model.CompleteMultipartUploadResp{URL: "https://cdn/a.bin"}, nil
		},
	}

	_, err := newTestUploader(third).Upload(context.Background(), strings.NewReader("0123"), &UploadOptions{
		Name: "a.bin", Size: 4, ForceMultipart: true, PartSize: 4, MaxParts: 8,
	})
	require.NoError(t, err)
	require.NotNil(t, completeReq)
	require.Len(t, completeReq.Parts, 1)
	require.NotEmpty(t, completeReq.Parts[0].ETag)
	require.True(t, strings.HasPrefix(completeReq.Parts[0].ETag, `"`), "兜底 ETag 应带引号")
}

// —— 11. Abort ——

func TestAbortDelegatesToThirdMgr(t *testing.T) {
	var gotReq *model.AbortMultipartUploadReq
	third := &stubThirdMgr{
		abortMultipartFn: func(_ context.Context, req *model.AbortMultipartUploadReq) error {
			gotReq = req
			return nil
		},
	}
	err := newTestUploader(third).Abort(context.Background(), "up-1", "a.bin", "https://cdn")
	require.NoError(t, err)
	require.NotNil(t, gotReq)
	require.Equal(t, "up-1", gotReq.UploadID)
	require.Equal(t, "a.bin", gotReq.Name)
	require.Equal(t, "https://cdn", gotReq.URLPrefix)

	// 透传底层错误
	third.abortMultipartFn = func(context.Context, *model.AbortMultipartUploadReq) error {
		return errors.New("abort boom")
	}
	err = newTestUploader(third).Abort(context.Background(), "up-1", "a.bin", "")
	require.ErrorContains(t, err, "abort boom")
}
