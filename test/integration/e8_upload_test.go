package integration

// E8 第三方存储上传（真实环境：im-api 签发预签名 → RustFS 存储）
//
// 与 E0-E6 的区别：E8 不需要 WS，只依赖 im-api HTTP（admin bootstrap 拿 user token）
// 与 RustFS。FileUploader 的字节传输目标就是 im-api 返回的 RustFS 预签名 URL。
//
// 运行方式（PowerShell）：
//
//	$env:GOIM_API_URL   = "http://127.0.0.1:18880/api/v1"  # 以实际 im-api 为准
//	$env:GOIM_SECRET    = "<admin secret>"
//	$env:GOIM_RUSTFS_URL = "http://127.0.0.1:9000"        # 可选，仅日志标注目标存储
//	go test ./test/integration/... -run TestE8 -v -count=1
//
// 环境变量缺失时 requireUploaderEnv 自动 Skip，不影响 `go test ./...`。
//
// 用例：
//   E8.1 小文件 FormData 直传 → Complete 后 URL 可下载，字节一致
//   E8.2 大文件 Multipart 分片 → PartLimit/PartSize 可用、分片齐全、进度到 100%、字节一致
//   E8.3 断点续传 → 首个分片完成后取消，ResumeState 非空；用同一状态续传成功且字节一致
//   E8.4 Abort → Initiate 后放弃上传，服务端清理调用成功

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/manager"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
)

const (
	uploadTimeout     = 5 * time.Minute
	multipartPartSize = 5 * 1024 * 1024
	multipartFileSize = 9*1024*1024 + 4321 // > DefaultMultipartThreshold(8MB)，PartSize 5MB → 2 片
)

// requireUploaderEnv 上传用例只需 im-api + admin secret（不需要 WS）
func requireUploaderEnv(t *testing.T) {
	t.Helper()
	if envAPIURL == "" || envSecret == "" {
		t.Skip("uploader integration env missing: set GOIM_API_URL / GOIM_SECRET")
	}
}

// newUploaderFixture 构造直连真实 im-api 的 ThirdMgr + FileUploader：
// admin token → 注册测试用户 → user token → 以该用户身份调 /third 接口。
func newUploaderFixture(t *testing.T) (context.Context, manager.ThirdMgr, manager.FileUploader, string) {
	t.Helper()
	requireUploaderEnv(t)

	if r := os.Getenv("GOIM_RUSTFS_URL"); r != "" {
		t.Logf("target object storage (RustFS): %s", r)
	}

	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	t.Cleanup(cancel)

	l := testLogger()
	tokens := transport.NewTokenStore(nil)
	httpClient := transport.NewHTTPClient(envAPIURL, tokens, l)
	authMgr := manager.NewAuthMgr(httpClient, tokens, nil, l)

	adminResp, err := authMgr.GetAdminToken(ctx, envSecret, adminUserID)
	require.NoError(t, err, "get admin token")
	require.NotEmpty(t, adminResp.Token)
	require.NoError(t, tokens.Set(adminUserID, platformID, adminResp.Token, adminResp.ExpireTimeSeconds))
	httpClient.SetCurrentUser(adminUserID, platformID)

	userID := uniqueUser("up")
	require.NoError(t, manager.NewUserMgr(httpClient, nil, l).Register(ctx, []*model.UserInfo{
		{UserID: userID, Nickname: userID, CreateTime: time.Now().UnixMilli()},
	}), "register user %s", userID)

	userResp, err := authMgr.GetUserToken(ctx, userID, platformID)
	require.NoError(t, err, "get user token for %s", userID)
	require.NotEmpty(t, userResp.Token)
	// GetUserToken 内部已写入 TokenStore；切换当前用户，后续 /third 请求携带 user token
	httpClient.SetCurrentUser(userID, platformID)

	third := manager.NewThirdMgr(httpClient, l)
	return ctx, third, manager.NewFileUploader(third, l), userID
}

// TestE8_UploaderFormDataSmallFile E8.1：小文件默认走 FormData 直传
func TestE8_UploaderFormDataSmallFile(t *testing.T) {
	ctx, third, uploader, userID := newUploaderFixture(t)

	data := uploadPayload(2*1024, 7)
	name := fmt.Sprintf("it-upload/%s/formdata-%d.bin", userID, time.Now().UnixNano())

	res, err := uploader.Upload(ctx, bytes.NewReader(data), &manager.UploadOptions{
		Name:        name,
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
		Cause:       "sdk-it-e8",
	})
	require.NoError(t, err, "FormData upload")
	require.NotNil(t, res)
	require.False(t, res.Multipart, "small file should use FormData path")
	require.NotEmpty(t, res.URL, "CompleteFormData should return accessible URL")
	require.Nil(t, res.ResumeState)

	verifyObject(t, third, name, res.URL, data)
	t.Logf("E8.1 ok: name=%s url=%s", name, res.URL)
}

// TestE8_UploaderMultipartLargeFile E8.2：大文件走 Multipart（真实分片 PUT）
func TestE8_UploaderMultipartLargeFile(t *testing.T) {
	ctx, third, uploader, userID := newUploaderFixture(t)

	limit, err := third.PartLimit(ctx)
	require.NoError(t, err, "PartLimit")
	require.Greater(t, limit.MaxNumSize, int32(0), "server max parts")
	recommended, err := third.PartSize(ctx, multipartFileSize)
	require.NoError(t, err, "PartSize")
	require.Greater(t, recommended.Size, int64(0), "server recommended part size")
	t.Logf("server limit: maxNumSize=%d maxPartSize=%d, recommended partSize=%d",
		limit.MaxNumSize, limit.MaxPartSize, recommended.Size)

	data := uploadPayload(multipartFileSize, 11)
	name := fmt.Sprintf("it-upload/%s/multipart-%d.bin", userID, time.Now().UnixNano())

	var mu sync.Mutex
	var maxUploaded int64
	res, err := uploader.Upload(ctx, bytes.NewReader(data), &manager.UploadOptions{
		Name:        name,
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
		Cause:       "sdk-it-e8",
		PartSize:    multipartPartSize,
		Concurrency: 3,
		OnProgress: func(uploaded, _ int64) {
			mu.Lock()
			if uploaded > maxUploaded {
				maxUploaded = uploaded
			}
			mu.Unlock()
		},
	})
	require.NoError(t, err, "Multipart upload")
	require.NotNil(t, res)
	require.True(t, res.Multipart, "large file should use Multipart path")
	require.NotEmpty(t, res.UploadID)
	require.Equal(t, int64(multipartPartSize), res.PartSize)
	require.Len(t, res.CompletedParts, 2, "9MB / 5MB = 2 parts")
	for _, p := range res.CompletedParts {
		require.NotEmpty(t, p.ETag, "part %d ETag", p.PartNumber)
	}
	require.NotEmpty(t, res.URL, "CompleteMultipartUpload should return final URL")
	require.Nil(t, res.ResumeState, "successful upload should clear resume state")

	mu.Lock()
	gotUploaded := maxUploaded
	mu.Unlock()
	require.Equal(t, int64(len(data)), gotUploaded, "progress should reach 100%")

	verifyObject(t, third, name, res.URL, data)
	t.Logf("E8.2 ok: uploadID=%s parts=%d url=%s", res.UploadID, len(res.CompletedParts), res.URL)
}

// TestE8_UploaderMultipartResume E8.3：首片完成后取消 → ResumeState → 续传成功
func TestE8_UploaderMultipartResume(t *testing.T) {
	ctx, third, uploader, userID := newUploaderFixture(t)

	data := uploadPayload(multipartFileSize, 23)
	name := fmt.Sprintf("it-upload/%s/resume-%d.bin", userID, time.Now().UnixNano())

	firstCtx, cancelFirst := context.WithCancel(ctx)
	t.Cleanup(cancelFirst)
	// 兜底：万一首片 PUT 一直不回调进度，20s 后也取消，避免测试挂死
	timer := time.AfterFunc(20*time.Second, cancelFirst)
	t.Cleanup(func() { timer.Stop() })

	var once sync.Once
	first, firstErr := uploader.Upload(firstCtx, bytes.NewReader(data), &manager.UploadOptions{
		Name:        name,
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
		Cause:       "sdk-it-e8-resume",
		PartSize:    multipartPartSize,
		Concurrency: 1, // 串行：首片进度回调内取消，确保第二片一定被中断
		PartRetry:   1,
		OnProgress: func(int64, int64) {
			once.Do(cancelFirst)
		},
	})
	require.Error(t, firstErr, "first attempt should be interrupted")
	require.True(t, errors.Is(firstErr, context.Canceled), "first attempt should end with ctx cancel, got %v", firstErr)
	require.NotNil(t, first)
	require.NotNil(t, first.ResumeState, "interrupted upload should return resume state")
	require.NotEmpty(t, first.ResumeState.UploadID)
	require.NotEmpty(t, first.ResumeState.CompletedParts, "at least one part should be done")
	require.Empty(t, first.URL, "interrupted upload should not return final URL")
	t.Logf("E8.3 first attempt interrupted: uploadID=%s done=%v",
		first.ResumeState.UploadID, first.ResumeState.CompletedParts)

	// 续传：不再 Initiate，只补传缺失分片，Complete 时带上已完成的
	resumed, err := uploader.Upload(ctx, bytes.NewReader(data), &manager.UploadOptions{
		Name:        name,
		Size:        int64(len(data)),
		ContentType: "application/octet-stream",
		Cause:       "sdk-it-e8-resume",
		Concurrency: 2,
		Resume:      first.ResumeState,
	})
	require.NoError(t, err, "resume upload")
	require.NotNil(t, resumed)
	require.NotEmpty(t, resumed.URL, "resume should finish with final URL")
	require.Nil(t, resumed.ResumeState, "resume success should clear state")

	verifyObject(t, third, name, resumed.URL, data)
	t.Logf("E8.3 ok: resumed uploadID=%s url=%s", resumed.UploadID, resumed.URL)
}

// TestE8_UploaderAbortMultipart E8.4：Initiate 后 Abort 能清理服务端上传会话
func TestE8_UploaderAbortMultipart(t *testing.T) {
	ctx, third, uploader, userID := newUploaderFixture(t)

	name := fmt.Sprintf("it-upload/%s/abort-%d.bin", userID, time.Now().UnixNano())
	initResp, err := third.InitiateMultipartUpload(ctx, &model.InitiateMultipartUploadReq{
		Size:     2 * 1024 * 1024,
		PartSize: 1024 * 1024,
		MaxParts: 2,
		Name:     name,
		Cause:    "sdk-it-e8-abort",
	})
	require.NoError(t, err, "InitiateMultipartUpload")
	require.NotNil(t, initResp)
	require.NotEmpty(t, initResp.Upload.UploadID)

	require.NoError(t, uploader.Abort(ctx, initResp.Upload.UploadID, name, ""), "Abort")
	t.Logf("E8.4 ok: aborted uploadID=%s name=%s", initResp.Upload.UploadID, name)
}

// —— 辅助 ——

// uploadPayload 生成确定性字节序列（避免依赖随机数，便于人眼比对/复现）
func uploadPayload(size int, seed int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte((i*31 + seed) % 251)
	}
	return b
}

// verifyObject 校验对象内容：
//  1. 直接 GET Complete 返回的 URL（若桶公开或 URL 自带预签名）
//  2. 回退 AccessURL 预签名下载
//  3. AccessURL 失败时用不存在的对象名探测端点：若同样失败说明服务端 access_url 整体故障，
//     记为服务端问题并跳过字节校验（上传 API 链路已成功）；若仅真实对象失败则报错
func verifyObject(t *testing.T, third manager.ThirdMgr, name, resultURL string, want []byte) {
	t.Helper()
	t.Logf("verify: name=%s completeURL=%s", name, resultURL)

	if resultURL != "" {
		body, status, err := httpGetBody(resultURL)
		if err == nil && status == http.StatusOK {
			require.Equal(t, want, body, "downloaded bytes mismatch from result URL")
			return
		}
		t.Logf("direct GET final URL failed (status=%d err=%v), fallback to AccessURL", status, err)
	}

	dlCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dl, err := third.AccessURL(dlCtx, &model.AccessURLReq{Name: name})
	require.NoError(t, err, "AccessURL for %s", name)
	require.NotEmpty(t, dl.URL)

	body, status, err := httpGetBody(dl.URL)
	require.NoError(t, err, "GET presigned URL")
	require.Equal(t, http.StatusOK, status, "presigned GET status")
	require.Equal(t, want, body, "downloaded bytes mismatch from presigned URL")
}

func httpGetBody(rawURL string) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}
