package manager

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"

	pbsdk "github.com/PaperMan11/goim/pkg/protocol/sdkws"
)

// stubWSTransport 仅实现 PullMsgBySeq 测试所需的 MessageTransport 行为（其余为空实现）
type stubWSTransport struct {
	resp *transport.Response
}

func (s *stubWSTransport) Connect(ctx context.Context) error { return nil }
func (s *stubWSTransport) Disconnect() error                 { return nil }
func (s *stubWSTransport) IsConnected() bool                 { return true }
func (s *stubWSTransport) Status() transport.TransportStatus { return transport.TransportStatusConnected }
func (s *stubWSTransport) SendRequest(ctx context.Context, req *transport.Request) error {
	return nil
}
func (s *stubWSTransport) SendRequestAndWait(ctx context.Context, req *transport.Request) (*transport.Response, error) {
	return s.resp, nil
}
func (s *stubWSTransport) Router() *transport.MessageRouter {
	return transport.NewMessageRouter(&logger.NopLogger{})
}
func (s *stubWSTransport) SetConnectParams(p transport.ConnectParams)    {}
func (s *stubWSTransport) SetOnConnecting(fn func())                     {}
func (s *stubWSTransport) SetOnReconnected(fn func(ctx context.Context)) {}
func (s *stubWSTransport) SetOnDisconnected(fn func())                   {}

// TestPullMsgBySeqFillsConversationID 拉取响应按 map key 回填 ConversationID，
// 否则 gap 拉取的消息入库后 conversation_id 为空（查询不到 + 唯一索引冲突）
func TestPullMsgBySeqFillsConversationID(t *testing.T) {
	respData, err := proto.Marshal(&pbsdk.PullMessageBySeqsResp{
		Msgs: map[string]*pbsdk.PullMsgs{
			"si_a_b": {
				Msgs: []*pbsdk.MsgData{
					{ClientMsgID: "m1", Seq: 1, SendID: "a", RecvID: "b", ContentType: 101},
					{ClientMsgID: "m2", Seq: 2, SendID: "b", RecvID: "a", ContentType: 101},
				},
				IsEnd:  true,
				EndSeq: 2,
			},
			"sg_1": {
				Msgs: []*pbsdk.MsgData{{ClientMsgID: "m3", Seq: 3, GroupID: "1", ContentType: 101}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	m := NewMsgMgr(nil, &stubWSTransport{resp: &transport.Response{Data: respData}}, nil, &logger.NopLogger{})
	result, err := m.PullMsgBySeq(context.Background(), "a", []*model.SeqRange{
		{ConversationID: "si_a_b", Begin: "1", End: "2"},
		{ConversationID: "sg_1", Begin: "3", End: "3"},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("convs = %d, want 2", len(result))
	}
	for convID, pr := range result {
		if len(pr.Msgs) == 0 {
			t.Fatalf("conv %s: no msgs", convID)
		}
		for _, mm := range pr.Msgs {
			if mm.ConversationID != convID {
				t.Fatalf("conv %s msg %s: ConversationID = %q, want %q", convID, mm.ClientMsgID, mm.ConversationID, convID)
			}
		}
	}
	pr := result["si_a_b"]
	if !pr.IsEnd || pr.EndSeq != "2" {
		t.Fatalf("si_a_b isEnd/endSeq: %v %q", pr.IsEnd, pr.EndSeq)
	}
}
