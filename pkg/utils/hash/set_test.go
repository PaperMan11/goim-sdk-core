package hash

import "testing"

// 测试集合哈希：无序输入应产生相同哈希值

func Test_HashStringSet_OrderInvariant(t *testing.T) {
	a := []string{"b", "a", "c"}
	b := []string{"a", "b", "c"}
	c := []string{"c", "a", "b"}
	ha := HashStringSet(a)
	hb := HashStringSet(b)
	hc := HashStringSet(c)
	if ha != hb || ha != hc {
		t.Errorf("HashStringSet 对相同元素不同顺序应产生相同哈希: a=%d b=%d c=%d", ha, hb, hc)
	}
}

func Test_HashStringSet_DifferentElements(t *testing.T) {
	a := []string{"a", "b", "c"}
	b := []string{"a", "b", "d"}
	if HashStringSet(a) == HashStringSet(b) {
		t.Errorf("HashStringSet 对不同元素集合应产生不同哈希")
	}
}

func Test_HashStringSet_Duplicate(t *testing.T) {
	// 含重复元素的情况：HashStringSet 不去重，["a","a"] 与 ["a"] 哈希不同
	// 这是预期行为（调用方负责去重），仅验证可正常工作
	_ = HashStringSet([]string{"a", "a"})
}

func Test_HashStringSet_Empty(t *testing.T) {
	if HashStringSet(nil) != 0 {
		t.Errorf("HashStringSet 对 nil 应返回 0")
	}
	if HashStringSet([]string{}) != 0 {
		t.Errorf("HashStringSet 对空切片应返回 0")
	}
}

func Test_HashStringSet_NotModifyInput(t *testing.T) {
	// 验证不修改传入切片（调用方可能复用）
	input := []string{"c", "b", "a"}
	orig := make([]string, len(input))
	copy(orig, input)
	_ = HashStringSet(input)
	for i := range input {
		if input[i] != orig[i] {
			t.Errorf("HashStringSet 不应修改输入切片: got %v, want %v", input, orig)
			break
		}
	}
}
