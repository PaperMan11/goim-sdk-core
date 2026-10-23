// Package storage 本地持久化接口 + 实现。
//
// MVP 只实现内存版本（单元测试 + 早期开发用）。
// SQLite（gorm + glebarez/sqlite，纯 Go 无 CGO）实现与业务数据表 → Phase 4.5；
// wasm IndexedDB 实现 → Phase 5。
//
// 设计原则：
//   - Storage 只管「轻量元数据」：最大 seq / versionID / version 数值
//   - 消息 / 会话缓存可选存；PullMsgBySeq 总是从服务端拉，Storage 不做消息缓存的硬依赖
//   - wasm 场景下 Storage 接口不变，底层换成 IndexedDB
package storage

import (
	"context"
	"sync"
)

// Storage 本地持久化接口
type Storage interface {
	// GetMaxSeqs 获取某用户所有会话的最大/最小 seq（MsgMgr.NewestSeq 结果持久化）
	GetMaxSeqs(userID string) (maxSeqs map[string]int64, minSeqs map[string]int64, err error)
	// SetMaxSeqs 写入最大/最小 seq
	SetMaxSeqs(userID string, maxSeqs, minSeqs map[string]int64) error

	// GetVersion 获取增量同步 versionID + version（按 entity 区分：joinGroups / groupMembers / friends / blacks / conversations）
	GetVersion(entity string) (versionID string, version uint64, err error)
	// SetVersion 写入 versionID + version
	SetVersion(entity, versionID string, version uint64) error

	// Clear 清理全部数据（Logout 时调用）
	Clear(ctx context.Context) error
}

// MemoryStorage 内存实现（默认，单元测试和早期开发用）
type MemoryStorage struct {
	mu sync.RWMutex

	// userID → maxSeqs/minSeqs
	seqs map[string]struct {
		max map[string]int64
		min map[string]int64
	}

	// entity → (versionID, version)
	versions map[string]struct {
		id      string
		version uint64
	}
}

// NewMemoryStorage 构造内存存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		seqs: make(map[string]struct{ max, min map[string]int64 }),
		versions: make(map[string]struct {
			id      string
			version uint64
		}),
	}
}

func (s *MemoryStorage) GetMaxSeqs(userID string) (map[string]int64, map[string]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seqs, ok := s.seqs[userID]
	if !ok {
		return nil, nil, nil
	}
	return seqs.max, seqs.min, nil
}

func (s *MemoryStorage) SetMaxSeqs(userID string, maxSeqs, minSeqs map[string]int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqs[userID] = struct {
		max map[string]int64
		min map[string]int64
	}{max: maxSeqs, min: minSeqs}
	return nil
}

func (s *MemoryStorage) GetVersion(entity string) (string, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.versions[entity]
	if !ok {
		return "", 0, nil
	}
	return v.id, v.version, nil
}

func (s *MemoryStorage) SetVersion(entity, versionID string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions[entity] = struct {
		id      string
		version uint64
	}{id: versionID, version: version}
	return nil
}

func (s *MemoryStorage) Clear(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqs = make(map[string]struct{ max, min map[string]int64 })
	s.versions = make(map[string]struct {
		id      string
		version uint64
	})
	return nil
}
