package manager

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/transport"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"

	pbmsg "github.com/PaperMan11/goim/pkg/protocol/msg"
	pbsdk "github.com/PaperMan11/goim/pkg/protocol/sdkws"
)

// DefaultMsgMgr MsgMgr 默认实现
//
// WS 请求（SendMsg / NewestSeq / PullMsgBySeqList）:
//
//	proto.Marshal → transport.Request{ReqIdentifier, Data} → ws.SendRequestAndWait → proto.Unmarshal
//
// HTTP 请求（RevokeMsg / MarkMsgsAsRead / ...）:
//
//	http.Post(ctx, "/msg/xxx", req, &resp)
type DefaultMsgMgr struct {
	http   transport.HTTPClient
	ws     transport.MessageTransport
	bus    *eventbus.EventBus
	logger logger.Logger

	// 发送者身份（登录后设置；Phase 4 由 DefaultClient 注入）
	senderID string
}

// NewMsgMgr 构造 MsgMgr
func NewMsgMgr(http transport.HTTPClient, ws transport.MessageTransport, bus *eventbus.EventBus, l logger.Logger) *DefaultMsgMgr {
	return &DefaultMsgMgr{http: http, ws: ws, bus: bus, logger: l}
}

func (m *DefaultMsgMgr) SetSenderID(userID string) { m.senderID = userID }

// —— WS 发送 / 拉取 ——

func (m *DefaultMsgMgr) SendMsg(ctx context.Context, msg *model.MsgData) (*model.SendMsgResp, error) {
	return m.sendWSMsg(ctx, msg, transport.WSSendMsg)
}

func (m *DefaultMsgMgr) SendSimpleMsg(ctx context.Context, msg *model.MsgData) (*model.SendMsgResp, error) {
	return m.sendWSMsg(ctx, msg, transport.WSSendMsg) // SendSimpleMsg 走同一通道
}

func (m *DefaultMsgMgr) sendWSMsg(ctx context.Context, msg *model.MsgData, reqID int) (*model.SendMsgResp, error) {
	if m.ws == nil {
		return nil, fmt.Errorf("websocket transport is nil")
	}

	req := &pbmsg.SendMsgReq{
		MsgData: toProtoMsgData(msg),
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal send msg req: %w", err)
	}

	resp, err := m.ws.SendRequestAndWait(ctx, &transport.Request{
		ReqIdentifier: reqID,
		SenderID:      m.senderID,
		Data:          data,
	})
	if err != nil {
		return nil, err
	}

	var wsResp pbsdk.UserSendMsgResp
	if err := proto.Unmarshal(resp.Data, &wsResp); err != nil {
		return nil, fmt.Errorf("unmarshal send msg resp: %w", err)
	}

	sendResp := &model.SendMsgResp{
		ServerMsgID: wsResp.ServerMsgID,
		ClientMsgID: wsResp.ClientMsgID,
		SendTime:    wsResp.SendTime,
	}

	if m.bus != nil {
		m.bus.Publish(events.EventMsgSent, events.Event{
			Type:      events.EventMsgSent,
			Timestamp: timex.UnixMilli(),
			Payload:   sendResp,
		})
	}
	return sendResp, nil
}

func (m *DefaultMsgMgr) BatchSendMsg(ctx context.Context, recvIDList []string, msg *model.MsgData) (*model.SendMsgResp, error) {
	req := struct {
		RecvIDList []string       `json:"recvIDList"`
		Msg        *model.MsgData `json:"msg"`
	}{RecvIDList: recvIDList, Msg: msg}
	var resp model.SendMsgResp
	if err := m.http.Post(ctx, "/msg/batch_send_msg", req, &resp); err != nil {
		return nil, fmt.Errorf("batch send msg: %w", err)
	}
	return &resp, nil
}

// NewestSeq 获取各会话最大/最小序列号（WS: WSGetNewestSeq=1001）
func (m *DefaultMsgMgr) NewestSeq(ctx context.Context) (map[string]int64, map[string]int64, error) {
	if m.ws == nil {
		return nil, nil, fmt.Errorf("websocket transport is nil")
	}

	req := &pbsdk.GetMaxSeqReq{UserID: m.senderID}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal get max seq req: %w", err)
	}

	resp, err := m.ws.SendRequestAndWait(ctx, &transport.Request{
		ReqIdentifier: transport.WSGetNewestSeq,
		SenderID:      m.senderID,
		Data:          data,
	})
	if err != nil {
		return nil, nil, err
	}

	var seqResp pbsdk.GetMaxSeqResp
	if err := proto.Unmarshal(resp.Data, &seqResp); err != nil {
		return nil, nil, fmt.Errorf("unmarshal get max seq resp: %w", err)
	}
	return seqResp.MaxSeqs, seqResp.MinSeqs, nil
}

// PullMsgBySeq 按序列号范围拉取消息（WS: WSPullMsgBySeqList=1002）
func (m *DefaultMsgMgr) PullMsgBySeq(ctx context.Context, userID string, seqRanges []*model.SeqRange, order int32) (map[string]*model.PullMsgsResult, error) {
	if m.ws == nil {
		return nil, fmt.Errorf("websocket transport is nil")
	}

	pbRanges := make([]*pbsdk.SeqRange, 0, len(seqRanges))
	for _, r := range seqRanges {
		begin := convert.ToInt64(r.Begin)
		end := convert.ToInt64(r.End)
		pbRanges = append(pbRanges, &pbsdk.SeqRange{
			ConversationID: r.ConversationID,
			Begin:          begin,
			End:            end,
			Num:            convert.ToInt64(r.Num),
		})
	}

	req := &pbsdk.PullMessageBySeqsReq{
		UserID:    userID,
		SeqRanges: pbRanges,
		Order:     pbsdk.PullOrder(order),
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal pull msg req: %w", err)
	}

	resp, err := m.ws.SendRequestAndWait(ctx, &transport.Request{
		ReqIdentifier: transport.WSPullMsgBySeqList,
		SenderID:      m.senderID,
		Data:          data,
	})
	if err != nil {
		return nil, err
	}

	var pullResp pbsdk.PullMessageBySeqsResp
	if err := proto.Unmarshal(resp.Data, &pullResp); err != nil {
		return nil, fmt.Errorf("unmarshal pull msg resp: %w", err)
	}

	result := make(map[string]*model.PullMsgsResult, len(pullResp.Msgs))
	for convID, pbMsgs := range pullResp.Msgs {
		result[convID] = &model.PullMsgsResult{
			Msgs:   fromProtoMsgList(pbMsgs.Msgs),
			IsEnd:  pbMsgs.IsEnd,
			EndSeq: convert.ToString(pbMsgs.EndSeq),
		}
	}
	return result, nil
}

// —— HTTP 接口 ——

func (m *DefaultMsgMgr) RevokeMsg(ctx context.Context, conversationID string, seq int64) error {
	req := struct {
		ConversationID string `json:"conversationID"`
		Seq            int64  `json:"seq"`
	}{ConversationID: conversationID, Seq: seq}
	if err := m.http.Post(ctx, "/msg/revoke_msg", req, nil); err != nil {
		return fmt.Errorf("revoke msg: %w", err)
	}
	if m.bus != nil {
		m.bus.Publish(events.EventMsgRevoked, events.Event{
			Type:      events.EventMsgRevoked,
			Timestamp: timex.UnixMilli(),
		})
	}
	return nil
}

func (m *DefaultMsgMgr) MarkMsgsAsRead(ctx context.Context, conversationID string, seqs []int64) error {
	req := struct {
		ConversationID string  `json:"conversationID"`
		Seqs           []int64 `json:"seqs"`
	}{ConversationID: conversationID, Seqs: seqs}
	if err := m.http.Post(ctx, "/msg/mark_msgs_as_read", req, nil); err != nil {
		return fmt.Errorf("mark msgs as read: %w", err)
	}
	return nil
}

func (m *DefaultMsgMgr) MarkConversationAsRead(ctx context.Context, conversationID string, hasReadSeq int64, seqs []int64) error {
	req := struct {
		ConversationID string  `json:"conversationID"`
		HasReadSeq     int64   `json:"hasReadSeq"`
		Seqs           []int64 `json:"seqs"`
	}{ConversationID: conversationID, HasReadSeq: hasReadSeq, Seqs: seqs}
	if err := m.http.Post(ctx, "/msg/mark_conversation_as_read", req, nil); err != nil {
		return fmt.Errorf("mark conversation as read: %w", err)
	}
	return nil
}

func (m *DefaultMsgMgr) GetConversationsHasReadAndMaxSeq(ctx context.Context, conversationIDs []string, returnPinned bool) (*model.SeqInfo, error) {
	req := struct {
		ConversationIDs []string `json:"conversationIDs"`
		ReturnPinned    bool     `json:"returnPinned"`
	}{ConversationIDs: conversationIDs, ReturnPinned: returnPinned}
	var resp model.SeqInfo
	if err := m.http.Post(ctx, "/msg/get_conversations_has_read_and_max_seq", req, &resp); err != nil {
		return nil, fmt.Errorf("get conversations has read and max seq: %w", err)
	}
	return &resp, nil
}

func (m *DefaultMsgMgr) ClearConversationMsg(ctx context.Context, conversationIDs []string, syncOpt *model.DeleteSyncOpt) error {
	req := struct {
		ConversationIDs []string             `json:"conversationIDs"`
		SyncOpt         *model.DeleteSyncOpt `json:"syncOpt"`
	}{ConversationIDs: conversationIDs, SyncOpt: syncOpt}
	if err := m.http.Post(ctx, "/msg/clear_conversation_msg", req, nil); err != nil {
		return fmt.Errorf("clear conversation msg: %w", err)
	}
	return nil
}

func (m *DefaultMsgMgr) DeleteMsgs(ctx context.Context, conversationID string, seqs []int64, syncOpt *model.DeleteSyncOpt) error {
	req := struct {
		ConversationID string               `json:"conversationID"`
		Seqs           []int64              `json:"seqs"`
		SyncOpt        *model.DeleteSyncOpt `json:"syncOpt"`
	}{ConversationID: conversationID, Seqs: seqs, SyncOpt: syncOpt}
	if err := m.http.Post(ctx, "/msg/delete_msgs", req, nil); err != nil {
		return fmt.Errorf("delete msgs: %w", err)
	}
	return nil
}

func (m *DefaultMsgMgr) DeleteMsgPhysicalBySeq(ctx context.Context, conversationID string, seqs []int64) error {
	req := struct {
		ConversationID string  `json:"conversationID"`
		Seqs           []int64 `json:"seqs"`
	}{ConversationID: conversationID, Seqs: seqs}
	if err := m.http.Post(ctx, "/msg/delete_msg_phsical_by_seq", req, nil); err != nil {
		return fmt.Errorf("delete msg physical by seq: %w", err)
	}
	return nil
}

func (m *DefaultMsgMgr) DeleteMsgPhysical(ctx context.Context, conversationIDs []string, timestamp int64) error {
	req := struct {
		ConversationIDs []string `json:"conversationIDs"`
		Timestamp       int64    `json:"timestamp"`
	}{ConversationIDs: conversationIDs, Timestamp: timestamp}
	if err := m.http.Post(ctx, "/msg/delete_msg_physical", req, nil); err != nil {
		return fmt.Errorf("delete msg physical: %w", err)
	}
	return nil
}

func (m *DefaultMsgMgr) SearchMsg(ctx context.Context, sendID, recvID string, contentType int32, sendTime int64, sessionType int32, pagination *model.RequestPagination) (*model.SearchMsgResult, error) {
	req := struct {
		SendID      string                   `json:"sendID"`
		RecvID      string                   `json:"recvID"`
		ContentType int32                    `json:"contentType"`
		SendTime    int64                    `json:"sendTime"`
		SessionType int32                    `json:"sessionType"`
		Pagination  *model.RequestPagination `json:"pagination"`
	}{SendID: sendID, RecvID: recvID, ContentType: contentType, SendTime: sendTime, SessionType: sessionType, Pagination: pagination}
	var resp model.SearchMsgResult
	if err := m.http.Post(ctx, "/msg/search_msg", req, &resp); err != nil {
		return nil, fmt.Errorf("search msg: %w", err)
	}
	return &resp, nil
}

func (m *DefaultMsgMgr) CheckMsgIsSendSuccess(ctx context.Context, clientMsgID string) (*model.MsgSendStatus, error) {
	req := struct {
		ClientMsgID string `json:"clientMsgID"`
	}{ClientMsgID: clientMsgID}
	var resp model.MsgSendStatus
	if err := m.http.Post(ctx, "/msg/check_msg_is_send_success", req, &resp); err != nil {
		return nil, fmt.Errorf("check msg is send success: %w", err)
	}
	return &resp, nil
}

// —— 辅助：model.MsgData ↔ proto sdkws.MsgData ——
//
// 注意：model.MsgData 是 SDK 本地 DTO，sdkws.MsgData 是服务端传输协议。
// 两者字段并非一一对应（如 model.MsgType 对应 proto.ContentType，model.Seqs 存 proto.Seq 的字符串形式）。

func toProtoMsgData(m *model.MsgData) *pbsdk.MsgData {
	if m == nil {
		return nil
	}
	return &pbsdk.MsgData{
		SendID:           m.SendID,
		RecvID:           m.RecvID,
		ClientMsgID:      m.ClientMsgID,
		ServerMsgID:      m.ServerMsgID,
		SenderPlatformID: m.SenderPlatformID,
		SenderNickname:   m.SenderNickname,
		SenderFaceURL:    m.SenderFaceURL,
		SessionType:      m.SessionType,
		MsgFrom:          m.MsgFrom,
		ContentType:      m.MsgType, // model.MsgType → proto.ContentType
		Content:          m.Content,
		SendTime:         m.CreateTime,
		Status:           m.Status,
		IsRead:           m.IsRead,
		AttachedInfo:     m.AttachedInfo,
		Ex:               m.Ex,
	}
}

func fromProtoMsgData(pm *pbsdk.MsgData) *model.MsgData {
	if pm == nil {
		return nil
	}
	return &model.MsgData{
		SendID:           pm.SendID,
		RecvID:           pm.RecvID,
		ClientMsgID:      pm.ClientMsgID,
		ServerMsgID:      pm.ServerMsgID,
		SenderPlatformID: pm.SenderPlatformID,
		SenderNickname:   pm.SenderNickname,
		SenderFaceURL:    pm.SenderFaceURL,
		SessionType:      pm.SessionType,
		MsgFrom:          pm.MsgFrom,
		MsgType:          pm.ContentType, // proto.ContentType → model.MsgType
		Content:          pm.Content,
		CreateTime:       pm.SendTime,
		Seqs:             convert.ToString(pm.Seq),
		Status:           pm.Status,
		IsRead:           pm.IsRead,
		AttachedInfo:     pm.AttachedInfo,
		Ex:               pm.Ex,
	}
}

func fromProtoMsgList(pbMsgs []*pbsdk.MsgData) []*model.MsgData {
	msgs := make([]*model.MsgData, 0, len(pbMsgs))
	for _, pm := range pbMsgs {
		msgs = append(msgs, fromProtoMsgData(pm))
	}
	return msgs
}
