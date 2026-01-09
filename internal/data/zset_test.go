package data

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

/* ---------- 小规模场景（元素 ≤128，走 listpack） ---------- */

func TestZSetSmall_BasicCRUD(t *testing.T) {
	zs := NewZSet()

	// ZAdd 新增
	if n := zs.ZAdd(false, false, false, 1, []byte("m1")); n != 1 {
		t.Error("small ZAdd new member should return 1")
	}
	if n := zs.ZAdd(false, false, false, 2, []byte("m2")); n != 1 {
		t.Error("small ZAdd new member should return 1")
	}
	// 覆盖
	if n := zs.ZAdd(false, false, false, 3, []byte("m1")); n != 0 {
		t.Error("small ZAdd overwrite should return 0")
	}
	// ZScore
	if s, ok := zs.ZScore([]byte("m1")); !ok || s != 3 {
		t.Errorf("small ZScore=%v,%v want 3,true", s, ok)
	}
	// ZRank
	if r, ok := zs.ZRank([]byte("m1")); !ok || r != 1 {
		t.Errorf("small ZRank=%v,%v want 1,true", r, ok)
	}
	if r, ok := zs.ZRank([]byte("m2")); !ok || r != 0 {
		t.Errorf("small ZRank=%v,%v want 0,true", r, ok)
	}
	// ZRem
	if n := zs.ZRem([]byte("m1")); n != 1 {
		t.Error("small ZRem should return 1")
	}
	if zs.ZCard() != 1 {
		t.Error("small ZCard should be 1 after rem")
	}
}

func TestZSetSmall_ZRange(t *testing.T) {
	zs := NewZSet()
	// 写入 10 个乱序分数
	for i := 0; i < 10; i++ {
		zs.ZAdd(false, false, false, float64(10-i), []byte("m"+itoa(i)))
	}
	// 正序 [0,9] 带分数
	arr := zs.ZRange(0, 9, true)
	if len(arr) != 20 {
		t.Fatalf("small ZRange withScores len=%d, want 20", len(arr))
	}
	// 检查顺序：分数 1~10
	for i := 0; i < 10; i++ {
		wantScore := float64(i + 1)
		gotScore, _ := strToF64(arr[i*2+1])
		if gotScore != wantScore {
			t.Errorf("small ZRange[%d] score=%v, want %v actual %v", i, gotScore, wantScore, gotScore)
		}
	}
	// 逆序 [0,2]
	rev := zs.ZRevRange(0, 2, false)
	if len(rev) != 3 {
		t.Fatalf("small ZRevRange len=%d, want 3", len(rev))
	}
	wantRev := [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")}
	for i := range rev {
		if !bytes.Equal(rev[i], wantRev[i]) {
			t.Errorf("small ZRevRange[%d]=%q, want %q", i, rev[i], wantRev[i])
		}
	}
}

func TestZSetSmall_ZCount(t *testing.T) {
	zs := NewZSet()
	for i := 1; i <= 10; i++ {
		zs.ZAdd(false, false, false, float64(i), []byte("m"+itoa(i)))
	}
	if c := zs.ZCount(3, 7); c != 5 {
		t.Errorf("small ZCount=%d, want 5", c)
	}
	if c := zs.ZCount(11, 20); c != 0 {
		t.Errorf("small ZCount out range=%d, want 0", c)
	}
}

func TestZSetSmall_ZIncrBy(t *testing.T) {
	zs := NewZSet()
	zs.ZAdd(false, false, false, 10, []byte("m"))
	newScore := zs.ZIncrBy(5, []byte("m"))
	if newScore != 15 {
		t.Errorf("small ZIncrBy=%v, want 15", newScore)
	}
	if s, _ := zs.ZScore([]byte("m")); s != 15 {
		t.Errorf("small ZScore after incr=%v, want 15", s)
	}
	// 对不存在 member 递增
	newScore2 := zs.ZIncrBy(3, []byte("new"))
	if newScore2 != 3 {
		t.Errorf("small ZIncrBy new=%v, want 3", newScore2)
	}
}

func TestZSetSmall_Edge(t *testing.T) {
	zs := NewZSet()
	// 空表
	if r, ok := zs.ZRank([]byte("no")); ok || r != -1 {
		t.Error("small ZRank on empty should return -1,false")
	}
	if c := zs.ZCount(-math.MaxFloat64, math.MaxFloat64); c != 0 {
		t.Error("small ZCount on empty should return 0")
	}
	arr := zs.ZRange(0, -1, false)
	if len(arr) != 0 {
		t.Error("small ZRange on empty should return empty")
	}
	// 负数区间
	zs.ZAdd(false, false, false, 1, []byte("m"))
	arr = zs.ZRange(-1, -2, false)
	if len(arr) != 0 {
		t.Error("small ZRange negative reverse should return empty")
	}
}

/* ---------- 大规模场景（元素 >128，触发 SkipList） ---------- */

func TestZSetLarge_BasicCRUD(t *testing.T) {
	zs := NewZSet()
	// 写入 200 个，强制升级
	for i := 0; i < 200; i++ {
		zs.ZAdd(false, false, false, float64(i*10), []byte("m"+itoa(i)))
	}
	if zs.ZCard() != 200 {
		t.Fatalf("large ZCard=%d, want 200", zs.ZCard())
	}
	// 验证升级后读写
	if s, ok := zs.ZScore([]byte("m199")); !ok || s != 1990 {
		t.Errorf("large ZScore=%v,%v want 1990,true", s, ok)
	}
	if n := zs.ZRem([]byte("m100")); n != 1 {
		t.Error("large ZRem should return 1")
	}
	if zs.ZCard() != 199 {
		t.Error("large ZCard should be 199 after rem")
	}
}

func TestZSetLarge_ZRange(t *testing.T) {
	zs := NewZSet()
	n := 500
	for i := 0; i < n; i++ {
		zs.ZAdd(false, false, false, float64(i), []byte("m"+itoa(i)))
	}
	// 取中段 [100,199] 带分数
	arr := zs.ZRange(100, 199, true)
	if len(arr) != 200 {
		t.Fatalf("large ZRange withScores len=%d, want 200", len(arr))
	}
	// 检查第一个
	if score, _ := strToF64(arr[1]); score != 100 {
		t.Errorf("large ZRange first score=%v, want 100", score)
	}
	// 逆序取前 10
	rev := zs.ZRevRange(0, 9, false)
	if len(rev) != 10 {
		t.Fatalf("large ZRevRange len=%d, want 10", len(rev))
	}
	wantFirst := []byte("m499")
	if !bytes.Equal(rev[0], wantFirst) {
		t.Errorf("large ZRevRange[0]=%q, want %q", rev[0], wantFirst)
	}
}

func TestZSetLarge_ZCount(t *testing.T) {
	zs := NewZSet()
	// 0~999
	for i := 0; i < 1000; i++ {
		zs.ZAdd(false, false, false, float64(i), []byte("m"+itoa(i)))
	}
	if c := zs.ZCount(100, 200); c != 101 {
		t.Errorf("large ZCount=%d, want 101", c)
	}
	if c := zs.ZCount(500.5, 600.5); c != 100 {
		t.Errorf("large ZCount float=%d, want 100", c)
	}
}

func TestZSetLarge_ZIncrBy(t *testing.T) {
	zs := NewZSet()
	// 写入 300 个
	for i := 0; i < 300; i++ {
		zs.ZAdd(false, false, false, float64(i), []byte("m"+itoa(i)))
	}
	// 对中间 member 递增
	newScore := zs.ZIncrBy(50.5, []byte("m150"))
	if newScore != 200.5 {
		t.Errorf("large ZIncrBy=%v, want 200.5", newScore)
	}
	// 验证排名变化
	rank, _ := zs.ZRank([]byte("m150"))
	// 分数 200.5 应该排在 200 名（0-based）
	if rank != 200 {
		t.Errorf("large ZRank after incr=%d, want 200", rank)
	}
}

/* ---------- 工具 ---------- */

func itoa(i int) string { return strconv.Itoa(i) }

func strToF64(b []byte) (float64, error) {
	return strconv.ParseFloat(string(b), 64)
}
