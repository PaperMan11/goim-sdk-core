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
	"time"

	"github.com/PaperMan11/goim-sdk-core/pkg/events"
	"github.com/PaperMan11/goim-sdk-core/pkg/logger"
	"github.com/PaperMan11/goim-sdk-core/pkg/model"
	"github.com/PaperMan11/goim-sdk-core/pkg/storage"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/eventbus"
	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

// gapBatchSize 每批拉取的消息条数
const gapBatchSize = 100

// 批内缺口补拉参数。背景：服务端发送链路写通改造后（ack 前同步落库），同会话并发
// 发送仍存在毫秒级落库乱序窗口——游标（maxSeq）已推进到 seq N，但 N 之前某个 seq
// 的 insert 尚在另一并发路径中提交。拉取批返回的 seq 不连续时，短暂退避重拉给并发
// 提交让路；重试耗尽仍缺视为永久缺失（seq 已分配但消息崩溃丢失/已删，对齐服务端
// 「补拉连续 N 次为空视为已删」约定），跳过防止游标永久停滞。
const (
	gapHoleRetries    = 3
	gapHoleRetryDelay = 200 * time.Millisecond
)

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
//  4. 批内缺口有界补拉：收到的 seq 不连续（并发同步落库乱序的瞬时空洞）时游标停在
//     缺口前重拉；耗尽仍缺跳过并告警（视为已删/永久缺失）；补拉请求失败则游标停在
//     缺口前，留待下次同步
//  5. 单会话失败不中断其余会话（错误聚合返回，已成功会话进度已持久化）
//  6. Publish(EventSeqSynced)（仅全部会话成功，"数据已就绪"语义不变）
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
				Begin:          next,
				End:            end,
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
			last := pr.Msgs[len(pr.Msgs)-1].Seq
			got := make(map[int64]struct{}, len(pr.Msgs))
			for _, m := range pr.Msgs {
				got[m.Seq] = struct{}{}
			}

			// 批内缺口检测：收到的 seq 在 [batchStart, last] 内不连续时，游标先停在
			// 首个缺口前落盘（补拉失败/部分成功时都不越过未确认区间），缺口区间
			// 有界补拉后再统一推进游标
			if hole := firstHole(got, batchStart, last); hole > 0 {
				localMax[convID] = max(localMax[convID], hole-1)
				if err := saveCursor(); err != nil {
					errs = append(errs, fmt.Errorf("conversation %s: %w", convID, err))
					break
				}
				hole, rerr := e.fillHole(ctx, convID, got, hole, last)
				if rerr != nil {
					// 补拉失败（未知状态不跳过）：游标已停在缺口前，本会话剩余
					// 批次留待下次同步；已入库消息下轮幂等重拉
					errs = append(errs, fmt.Errorf("sync: hole retry %s [%d,%d]: %w", convID, hole, last-1, rerr))
					break
				}
				if hole > 0 && e.logger != nil {
					e.logger.Warnf("sync: conversation %s hole seq %d (batch [%d,%d]) unfilled after %d retries, skipped",
						convID, hole, batchStart, last, gapHoleRetries)
				}
			}
			localMax[convID] = max(localMax[convID], last)

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

// fillHole 对批内缺口区间 [hole, last-1] 做有界补拉（last 本身已收到，区间宽度
// 不超过单批）。瞬时空洞（并发同步落库乱序）退避重拉即可补齐；耗尽重试仍缺则
// 放弃，返回剩余的首个缺口 seq（调用方跳过，游标越过缺口推进）。
// 补拉请求/入库失败返回错误——未知状态不跳过，调用方将游标停在缺口前留待下次同步。
// got 为本批已收到的 seq 集合，补拉结果原地累积其中。
func (e *SeqSyncEngine) fillHole(ctx context.Context, convID string, got map[int64]struct{}, hole, last int64) (int64, error) {
	for attempt := 0; attempt < gapHoleRetries; attempt++ {
		time.Sleep(gapHoleRetryDelay)
		result, err := e.msg.PullMsgBySeq(ctx, e.owner, []*model.SeqRange{{
			ConversationID: convID,
			Begin:          hole,
			End:            last - 1,
			Num:            gapBatchSize,
			Order:          model.PullOrderAsc,
		}}, model.PullOrderAsc)
		if err != nil {
			return hole, err
		}
		pr := result[convID]
		if pr == nil || len(pr.Msgs) == 0 {
			continue
		}
		if err := e.stor.UpsertMsgs(e.owner, pr.Msgs); err != nil {
			return hole, err
		}
		for _, m := range pr.Msgs {
			got[m.Seq] = struct{}{}
		}
		if hole = firstHole(got, hole, last); hole == 0 {
			return 0, nil // 缺口已补齐
		}
	}
	return hole, nil
}

// firstHole 返回 [begin, end] 内首个不在 got 中的 seq（0 表示连续无洞；
// begin > end 时无洞）
func firstHole(got map[int64]struct{}, begin, end int64) int64 {
	for s := begin; s <= end; s++ {
		if _, ok := got[s]; !ok {
			return s
		}
	}
	return 0
}
