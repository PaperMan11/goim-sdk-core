// Package sync 本地数据同步引擎（Phase 4.5）。
//
// 两个引擎：
//   - SeqSyncEngine：消息 seq gap 拉取（WS 1001/1002），登录后 / 重连后执行
//   - EntitySyncEngine：五类实体（好友 / 黑名单 / 群 / 群成员 / 会话）游标增量 + IDHash 全量回退
//
// 协议对齐服务端 docs/data-sync.md：
//   - 增量接口一次返回全部变更（无分批），resp 带 version + versionID 写回本地游标
//   - resp.full == true → 版本线变更 → GetFull*IDs(idHash) 校验：
//     equal=true → 本地 ID 集合与服务端一致，仅对齐游标
//     equal=false → 清本地表 + 分页全量拉取 + 写新游标
package sync

import (
	"context"
	"errors"
	"fmt"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/convert"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// gapBatchSize 每批拉取的消息条数
const gapBatchSize = 100

// MsgSource SeqSyncEngine 的最小依赖（manager.MsgMgr 的子集）
type MsgSource interface {
	NewestSeq(ctx context.Context) (maxSeqs, minSeqs map[string]int64, err error)
	PullMsgBySeq(ctx context.Context, userID string, seqRanges []*model.SeqRange, order int32) (map[string]*model.PullMsgsResult, error)
}

// SeqSyncEngine 消息 seq 同步引擎
type SeqSyncEngine struct {
	msg    MsgSource
	stor   storage.Storage
	owner  string
	bus    *eventbus.EventBus
	logger logger.Logger
}

// NewSeqSyncEngine 构造消息同步引擎
func NewSeqSyncEngine(msg MsgSource, stor storage.Storage, owner string, bus *eventbus.EventBus, l logger.Logger) *SeqSyncEngine {
	return &SeqSyncEngine{msg: msg, stor: stor, owner: owner, bus: bus, logger: l}
}

// Sync 执行 gap 拉取：
//
//  1. NewestSeq → 服务端各会话 maxSeq/minSeq
//  2. 对比本地游标 → 缺口 [localMax+1, serverMax]（本地无记录从 serverMin 开始）
//  3. 分批 PullMsgBySeq → UpsertMsgs → 游标逐批落盘（commit-last：游标写在消息入库后，
//     失败重试只重拉未完成批次，不重复已完成会话/批次）
//  4. 单会话失败不中断其余会话（错误聚合返回，已成功会话进度已持久化）
//  5. Publish(EventSeqSynced)（仅全部会话成功，"数据已就绪"语义不变）
func (e *SeqSyncEngine) Sync(ctx context.Context) error {
	serverMax, serverMin, err := e.msg.NewestSeq(ctx)
	if err != nil {
		return fmt.Errorf("sync: newest seq: %w", err)
	}
	localMax, localMin, err := e.stor.GetMaxSeqs(e.owner)
	if err != nil {
		return fmt.Errorf("sync: get local seqs: %w", err)
	}
	if localMax == nil {
		localMax = map[string]int64{}
	}
	if localMin == nil {
		localMin = map[string]int64{}
	}

	// saveCursor 渐进落盘：每批消息入库后调用；失败重试从已持久化游标续拉
	saveCursor := func() error {
		if err := e.stor.SetMaxSeqs(e.owner, localMax, localMin); err != nil {
			return fmt.Errorf("sync: set local seqs: %w", err)
		}
		return nil
	}

	var errs []error
	minAdvanced := false
	for convID, svMax := range serverMax {
		// min 前移挪进会话循环：无 gap 的会话也能持久化 min 前移
		if svMin := serverMin[convID]; svMin > localMin[convID] {
			localMin[convID] = svMin
			minAdvanced = true
		}
		start := localMax[convID]
		if start == 0 {
			start = serverMin[convID] // 本地无该会话记录：从服务端 min 开始
			if start > 0 {
				start-- // 区间改为闭区间 [start, svMax]
			}
		}
		if svMax <= start {
			// 无缺口：仅 min 有前移时落盘一次（稳态零写盘）
			if minAdvanced {
				if err := saveCursor(); err != nil {
					errs = append(errs, fmt.Errorf("conversation %s: %w", convID, err))
				}
				minAdvanced = false
			}
			continue
		}

		next := start + 1
		for next <= svMax {
			batchStart := next // 本批起始 seq（防御分支用：拉取后游标必须越过它）
			end := next + gapBatchSize - 1
			if end > svMax {
				end = svMax
			}
			result, err := e.msg.PullMsgBySeq(ctx, e.owner, []*model.SeqRange{{
				ConversationID: convID,
				Begin:          convert.ToString(next),
				End:            convert.ToString(end),
				Num:            gapBatchSize,
				Order:          model.PullOrderAsc,
			}}, model.PullOrderAsc)
			if err != nil {
				// 会话隔离：本会话失败，记录错误继续其余会话
				errs = append(errs, fmt.Errorf("sync: pull msgs %s [%d,%d]: %w", convID, next, end, err))
				break
			}
			pr := result[convID]
			if pr == nil || len(pr.Msgs) == 0 {
				// 服务端无更多消息（seq 空洞或瞬时异常）：游标停在已收到的最大 seq，
				// 缺口留待下次同步重试（幂等）
				break
			}
			if err := e.stor.UpsertMsgs(e.owner, pr.Msgs); err != nil {
				errs = append(errs, fmt.Errorf("sync: upsert msgs %s: %w", convID, err))
				break
			}
			last := pr.Msgs[len(pr.Msgs)-1]
			localMax[convID] = max(localMax[convID], convert.ToInt64(last.Seqs))

			if err := saveCursor(); err != nil {
				errs = append(errs, fmt.Errorf("conversation %s: %w", convID, err))
				break
			}

			if pr.IsEnd {
				break // 服务端标记本区间已拉完
			}
			next = localMax[convID] + 1
			if next <= batchStart {
				// 防御：游标未前进（last.Seq 异常或解析失败）→ 跳到本批 end，避免死循环。
				// 必须对比本批起始 batchStart：若对比缺口起点 start，只能保护第一批，
				// 后续批次游标停滞时会无限拉取同一区间
				localMax[convID] = end
				next = end + 1
			}
		}
	}

	// 尾部落盘：min 前移未被批内落盘覆盖时（如全部会话无 gap、或失败会话的 min 前移）
	if minAdvanced {
		if err := saveCursor(); err != nil {
			errs = append(errs, err)
		}
	}

	// 全部会话成功才发事件（"数据已就绪"语义）；失败聚合返回，重试只拉未完成部分
	if err := errors.Join(errs...); err != nil {
		return err
	}

	if e.bus != nil {
		e.bus.Publish(events.EventSeqSynced, events.Event{
			Type:      events.EventSeqSynced,
			Timestamp: timex.UnixMilli(),
		})
	}
	return nil
}
