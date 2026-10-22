package randx

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSecureBytes(t *testing.T) {
	b0, err := SecureBytes(0)
	assert.NoError(t, err)
	assert.NotNil(t, b0)
	assert.Equal(t, 0, len(b0), "n=0 应返回空切片")

	b1, err := SecureBytes(32)
	assert.NoError(t, err)
	assert.Equal(t, 32, len(b1))

	b2, err := SecureBytes(32)
	assert.NoError(t, err)
	assert.Equal(t, 32, len(b2))
	assert.NotEqual(t, b1, b2, "两次 SecureBytes(32) 不应返回相同字节")
}

func TestSecureString(t *testing.T) {
	s0, err := SecureString(0, CharsAlphaNum)
	assert.NoError(t, err)
	assert.Equal(t, "", s0)

	s1, err := SecureString(16, "")
	assert.NoError(t, err)
	assert.Equal(t, "", s1, "空 chars 应返回空串")

	const n = 64
	s2, err := SecureString(n, CharsAlphaNum)
	assert.NoError(t, err)
	assert.Equal(t, n, len(s2))
	for _, c := range s2 {
		assert.True(t, strings.ContainsRune(CharsAlphaNum, c), "SecureString 每个字符必须属于 chars: %c", c)
	}

	s3, err := SecureString(n, CharsAlphaNum)
	assert.NoError(t, err)
	assert.NotEqual(t, s2, s3, "两次 SecureString 不应相等")

	longChars := CharsAlphaNum + CharsAlphaNum + CharsAlphaNum + CharsAlphaNum + CharsAlphaNum
	assert.True(t, len(longChars) > 0xFF, "构造 >256 字符集以走大字符集分支 (len=%d)", len(longChars))
	s4, err := SecureString(32, longChars)
	assert.NoError(t, err)
	assert.Equal(t, 32, len(s4))
	for _, c := range s4 {
		assert.True(t, strings.ContainsRune(longChars, c), "SecureString 大字符集分支字符越界: %c", c)
	}
}

func TestSecureHex(t *testing.T) {
	s0, err := SecureHex(0)
	assert.NoError(t, err)
	assert.Equal(t, "", s0)

	const n = 30
	s1, err := SecureHex(n)
	assert.NoError(t, err)
	assert.Equal(t, n, len(s1))
	for _, c := range s1 {
		assert.True(t, strings.ContainsRune(CharsHex, c), "SecureHex 非十六进制字符: %c", c)
	}
}

func TestIntn(t *testing.T) {
	assert.Equal(t, 0, Intn(0), "n<=0 直接返回 0")
	assert.Equal(t, 0, Intn(-5), "n<=0 直接返回 0")

	const samples = 50000
	const bound = 100
	minHit, maxHit := false, false
	for i := 0; i < samples; i++ {
		v := Intn(bound)
		assert.True(t, v >= 0 && v < bound, "Intn(%d) 返回 %d 越界", bound, v)
		if v == 0 {
			minHit = true
		}
		if v == bound-1 {
			maxHit = true
		}
	}
	assert.True(t, minHit, "样本 %d 次应至少命中 0 端点一次", samples)
	assert.True(t, maxHit, "样本 %d 次应至少命中 %d 端点一次", samples, bound-1)
}

func TestInt31n(t *testing.T) {
	assert.Equal(t, int32(0), Int31n(0))
	const bound int32 = 10000
	for i := 0; i < 10000; i++ {
		v := Int31n(bound)
		assert.True(t, v >= 0 && v < bound, "Int31n(%d) 返回 %d 越界", bound, v)
	}
}

func TestInt63n(t *testing.T) {
	assert.Equal(t, int64(0), Int63n(0))
	const bound int64 = 100000
	for i := 0; i < 10000; i++ {
		v := Int63n(bound)
		assert.True(t, v >= 0 && v < bound, "Int63n(%d) 返回 %d 越界", bound, v)
	}
}

func TestIntnRange(t *testing.T) {
	assert.Equal(t, 5, IntnRange(5, 5), "min==max 应返回 min")
	assert.Equal(t, 8, IntnRange(8, 3), "min>max 应返回 min")

	const min, max = 10, 100
	const samples = 50000
	minHit, maxHit := false, false
	for i := 0; i < samples; i++ {
		v := IntnRange(min, max)
		assert.True(t, v >= min && v < max, "IntnRange(%d,%d) 返回 %d 越界", min, max, v)
		if v == min {
			minHit = true
		}
		if v == max-1 {
			maxHit = true
		}
	}
	assert.True(t, minHit, "IntnRange 端点 min 未命中")
	assert.True(t, maxHit, "IntnRange 端点 max-1 未命中")
}

func TestIntRangeInclusive(t *testing.T) {
	assert.Equal(t, 7, IntRangeInclusive(7, 7))
	assert.Equal(t, 9, IntRangeInclusive(9, 2))

	const min, max = 1, 5
	const samples = 50000
	hitCount := make(map[int]bool, 5)
	for i := 0; i < samples; i++ {
		v := IntRangeInclusive(min, max)
		assert.True(t, v >= min && v <= max, "IntRangeInclusive(%d,%d) 返回 %d 越界", min, max, v)
		hitCount[v] = true
	}
	for expect := min; expect <= max; expect++ {
		assert.True(t, hitCount[expect], "IntRangeInclusive 应覆盖端点 %d", expect)
	}
}

func TestFloat64(t *testing.T) {
	const samples = 20000
	for i := 0; i < samples; i++ {
		v := Float64()
		assert.True(t, v >= 0.0 && v < 1.0, "Float64 返回 %f 越界", v)
	}
}

func TestBool(t *testing.T) {
	const samples = 20000
	trueCount := 0
	for i := 0; i < samples; i++ {
		b := Bool()
		if b {
			trueCount++
		}
	}
	ratio := float64(trueCount) / samples
	assert.True(t, ratio > 0.4 && ratio < 0.6, "Bool 分布异常: true ratio=%f", ratio)
}

func TestJitterInt(t *testing.T) {
	t.Run("边界情况", func(t *testing.T) {
		assert.Equal(t, 0, JitterInt(0, 10), "base<=0 直接返回 0")
		assert.Equal(t, 0, JitterInt(-3, 10), "base<=0 直接返回 0")
		assert.Equal(t, 100, JitterInt(100, 0), "ratio<=0 直接返回 base")
		assert.Equal(t, 100, JitterInt(100, -5), "ratio<=0 直接返回 base")

		base := 100
		ratio := 200
		delta := base * 100 / 100
		for i := 0; i < 20; i++ {
			v := JitterInt(base, ratio)
			assert.True(t, v >= base-delta && v <= base+delta,
				"ratio>100 截断至 100 后仍越界: %d", v)
		}
	})

	t.Run("范围校验 ratio=10", func(t *testing.T) {
		const base = 1000
		const ratio = 10
		delta := base * ratio / 100
		const samples = 50000
		minHit, maxHit := false, false
		for i := 0; i < samples; i++ {
			v := JitterInt(base, ratio)
			assert.True(t, v >= base-delta && v <= base+delta,
				"JitterInt(%d,%d)=%d 超出 [%d,%d]", base, ratio, v, base-delta, base+delta)
			if v == base-delta {
				minHit = true
			}
			if v == base+delta {
				maxHit = true
			}
		}
		assert.True(t, minHit, "JitterInt 未命中下界")
		assert.True(t, maxHit, "JitterInt 未命中上界")
	})

	t.Run("delta 至少为 1", func(t *testing.T) {
		const base = 5
		results := make(map[int]bool)
		for i := 0; i < 2000; i++ {
			v := JitterInt(base, 10)
			results[v] = true
		}
		assert.GreaterOrEqual(t, len(results), 2,
			"很小的 base 在 delta 至少 1 的保证下也应能产生多种值")
	})
}

func TestJitterDuration(t *testing.T) {
	t.Run("边界情况", func(t *testing.T) {
		assert.Equal(t, time.Duration(0), JitterDuration(0, 10))
		assert.Equal(t, 5*time.Second, JitterDuration(5*time.Second, 0))
		assert.Equal(t, 5*time.Second, JitterDuration(5*time.Second, -1))
	})

	t.Run("范围校验", func(t *testing.T) {
		base := 10 * time.Second
		ratio := 20
		deltaNs := int64(base) * int64(ratio) / 100
		for i := 0; i < 10000; i++ {
			v := JitterDuration(base, ratio)
			assert.True(t, v >= base-time.Duration(deltaNs) && v <= base+time.Duration(deltaNs),
				"JitterDuration 结果越界: %v", v)
		}
	})
}

func TestString(t *testing.T) {
	assert.Equal(t, "", String(0, CharsAlpha))
	assert.Equal(t, "", String(10, ""))
	assert.Equal(t, "", String(0, ""))

	chars := "abcdef"
	const n = 40
	for i := 0; i < 100; i++ {
		s := String(n, chars)
		assert.Equal(t, n, len(s))
		for _, c := range s {
			assert.True(t, strings.ContainsRune(chars, c), "String 字符越界: %c", c)
		}
	}
}

func TestAlphaString(t *testing.T) {
	const n = 32
	s := AlphaString(n)
	assert.Equal(t, n, len(s))
	for _, c := range s {
		assert.True(t, strings.ContainsRune(CharsAlpha, c), "AlphaString 非字母字符: %c", c)
	}
}

func TestAlphaNumString(t *testing.T) {
	const n = 64
	s := AlphaNumString(n)
	assert.Equal(t, n, len(s))
	for _, c := range s {
		assert.True(t, strings.ContainsRune(CharsAlphaNum, c), "AlphaNumString 非字母数字字符: %c", c)
	}
}

func TestHexString(t *testing.T) {
	assert.Equal(t, "", HexString(0))
	for _, n := range []int{1, 5, 16, 31, 32, 64} {
		s := HexString(n)
		assert.Equal(t, n, len(s), "HexString(%d) 长度不匹配", n)
		for _, c := range s {
			assert.True(t, strings.ContainsRune(CharsHex, c), "HexString 非 hex 字符: %c", c)
		}
	}
}

func TestDuration(t *testing.T) {
	min, max := 5*time.Second, 10*time.Second
	assert.Equal(t, min, Duration(min, min))
	assert.Equal(t, 8*time.Second, Duration(8*time.Second, 3*time.Second))

	const samples = 50000
	for i := 0; i < samples; i++ {
		v := Duration(min, max)
		assert.True(t, v >= min && v < max, "Duration 结果越界: %v", v)
	}

	tinyMin := time.Duration(0)
	tinyMax := time.Duration(2)
	hitMin := false
	for i := 0; i < 20000; i++ {
		if Duration(tinyMin, tinyMax) == tinyMin {
			hitMin = true
			break
		}
	}
	assert.True(t, hitMin, "Duration 应能命中下界端点")
}

func TestPerm(t *testing.T) {
	assert.Nil(t, Perm(0), "n<=0 返回 nil")
	assert.Nil(t, Perm(-3))

	for _, n := range []int{1, 5, 20, 100} {
		p := Perm(n)
		assert.Equal(t, n, len(p))
		seen := make(map[int]bool, n)
		for _, v := range p {
			assert.True(t, v >= 0 && v < n, "Perm 元素越界: %d", v)
			assert.False(t, seen[v], "Perm 有重复元素: %d", v)
			seen[v] = true
		}
		for i := 0; i < n; i++ {
			assert.True(t, seen[i], "Perm 缺失元素: %d", i)
		}
	}

	if Perm(8) != nil {
		changed := false
		for i := 0; i < 20; i++ {
			p := Perm(4)
			q := Perm(4)
			if len(p) != len(q) {
				continue
			}
			ne := false
			for j := 0; j < len(p); j++ {
				if p[j] != q[j] {
					ne = true
					break
				}
			}
			if ne {
				changed = true
				break
			}
		}
		assert.True(t, changed, "Perm 连续调用应产生不同排列")
	}
}

func TestShuffle(t *testing.T) {
	assert.NotPanics(t, func() { Shuffle(0, nil) }, "n<=1 或 swap==nil 都不 panic")
	assert.NotPanics(t, func() { Shuffle(1, nil) })
	swap := func(i, j int) {}
	assert.NotPanics(t, func() { Shuffle(0, swap) })

	orig := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	arr := make([]int, len(orig))
	copy(arr, orig)
	Shuffle(len(arr), func(i, j int) { arr[i], arr[j] = arr[j], arr[i] })

	totalSame := 0
	for i := range orig {
		if orig[i] == arr[i] {
			totalSame++
		}
	}
	assert.LessOrEqual(t, totalSame, len(orig)-2, "打乱后应至少改变 2 个位置（极低概率失败）")

	counts := make(map[int]int, len(orig))
	for _, v := range arr {
		counts[v]++
	}
	for _, v := range orig {
		assert.Equal(t, 1, counts[v], "打乱后元素集合应与原集合一致，缺失/重复: %d", v)
	}
}

func TestConcurrentSafe(t *testing.T) {
	const goroutines = 128
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	panicked := make(chan string, goroutines)
	results := make(chan int, goroutines*2)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked <- "panic"
				}
			}()
			localSum := 0
			for i := 0; i < perGoroutine; i++ {
				localSum += Intn(10)
				_ = JitterInt(100, 10)
				_ = AlphaString(8)
				_ = IntnRange(0, 100)
				_ = Bool()
				_ = Float64()
			}
			results <- localSum
		}()
	}

	wg.Wait()
	close(panicked)
	close(results)

	for p := range panicked {
		t.Fatalf("并发调用期间出现 %s", p)
	}

	sumCount := 0
	for range results {
		sumCount++
	}
	assert.Equal(t, goroutines, sumCount, "每个 goroutine 都应产生一个结果")
}
