package hash

import (
	"hash/fnv"
	"sort"
	"strings"
)

/* 对字符串切片做集合哈希 */

// HashStringSet 对字符串切片做"集合哈希"：内部先排序再求 FNV-1a 64 位哈希。
//
// 语义：相同元素集合（不论输入顺序）产生相同哈希值，用于全量同步的等值比较。
// 例：["b","a","c"] 与 ["a","b","c"] 与 ["c","a","b"] 均返回相同结果。
//
// 使用场景：客户端持有上一次同步的哈希值，服务端计算当前 ID 集合的哈希值，
// 二者相等即可跳过全量数据下发，仅走增量同步。
func HashStringSet(ids []string) uint64 {
	if len(ids) == 0 {
		return 0
	}
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)
	h := fnv.New64a()
	h.Write([]byte(strings.Join(sorted, ",")))
	return h.Sum64()
}
