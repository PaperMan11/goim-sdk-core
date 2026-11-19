package manager

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"

	pbsdk "github.com/PaperMan11/goim/pkg/protocol/sdkws"
)

// TestPullHistoryMsgsHTTP 校验 HTTP 历史分页：请求字段（begin/end/num/order）与响应解码。
func TestPullHistoryMsgsHTTP(t *testing.T) {
	var gotReq pbsdk.PullMessageBySeqsReq

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/msg/pull_msg_by_seq" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Errorf("decode req: %v", err)
		}
		payload := map[string]interface{}{
			"msgs": map[string]interface{}{
				"si_x": map[string]interface{}{
					"Msgs": []map[string]interface{}{
						{"clientMsgID": "c8", "serverMsgID": "s8", "seq": 8, "contentType": 101, "content": "m8"},
						{"clientMsgID": "c7", "serverMsgID": "s7", "seq": 7, "contentType": 101, "content": "m7"},
					},
					"isEnd":  false,
					"endSeq": 100,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "msg": "", "data": payload})
	}))
	defer srv.Close()

	mgr := NewMsgMgr(transport.NewHTTPClient(srv.URL+"/api/v1", transport.NewTokenStore(nil), &logger.NopLogger{}),
		nil, nil, &logger.NopLogger{})
	mgr.SetSenderID("u1")

	page, err := mgr.PullHistoryMsgs(context.Background(), "si_x", 1, 0, 2, 1)
	require.NoError(t, err)
	require.Len(t, page.Msgs, 2)
	require.Equal(t, int64(7), page.OldestSeq)
	require.Equal(t, int64(8), page.NewestSeq)
	require.Equal(t, "si_x", page.Msgs[0].ConversationID, "会话 ID 由请求回填")

	// 请求参数：end<=0 转 MaxInt64，num 走 count，order=1（降序）
	require.Len(t, gotReq.SeqRanges, 1)
	require.Equal(t, int64(1), gotReq.SeqRanges[0].Begin)
	require.Equal(t, int64(math.MaxInt64), gotReq.SeqRanges[0].End)
	require.Equal(t, int64(2), gotReq.SeqRanges[0].Num)
	require.Equal(t, pbsdk.PullOrder_PullOrderDesc, gotReq.Order)
	require.Equal(t, "u1", gotReq.UserID)
}
