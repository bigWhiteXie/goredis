package data

import (
	"bytes"
	"reflect"
	"sort"
	"testing"
)

/* ---------- 基础 CRUD ---------- */

func TestNewRedisHash(t *testing.T) {
	h := NewRedisHash()
	if h.HLEN() != 0 {
		t.Fatalf("new hash should be empty, got len=%d", h.HLEN())
	}
}

func TestHSetHGet(t *testing.T) {
	h := NewRedisHash()

	// 新增字段
	if n := h.HSet("f1", []byte("v1")); n != 1 {
		t.Error("HSet new field should return 1")
	}
	// 读取
	v, ok := h.HGet("f1")
	if !ok || !bytes.Equal(v, []byte("v1")) {
		t.Error("HGet failed")
	}
	// 覆盖旧值
	if n := h.HSet("f1", []byte("new")); n != 0 {
		t.Error("HSet overwrite should return 0")
	}
	v, _ = h.HGet("f1")
	if !bytes.Equal(v, []byte("new")) {
		t.Error("HGet after overwrite failed")
	}
	// 不存在字段
	if _, ok := h.HGet("no"); ok {
		t.Error("HGet non-exist should return false")
	}
}

func TestHDel(t *testing.T) {
	h := NewRedisHash()
	h.HSet("f1", []byte("v1"))
	h.HSet("f2", []byte("v2"))

	// 删除 2 个
	if n := h.HDel("f1", "f2"); n != 2 {
		t.Errorf("HDel deleted=%d, want 2", n)
	}
	if h.HLEN() != 0 {
		t.Error("hash should be empty after HDel")
	}
	// 再删不存在字段
	if n := h.HDel("f1"); n != 0 {
		t.Error("HDel non-exist should return 0")
	}
}

func TestHExists(t *testing.T) {
	h := NewRedisHash()
	h.HSet("f", []byte("v"))
	if !h.HExists("f") {
		t.Error("HExists exist should return true")
	}
	if h.HExists("no") {
		t.Error("HExists non-exist should return false")
	}
}

func TestHLEN(t *testing.T) {
	h := NewRedisHash()
	if h.HLEN() != 0 {
		t.Error("HLEN on empty hash should return 0")
	}
	for i := 0; i < 100; i++ {
		h.HSet(string(rune(i)), []byte("val"))
	}
	if h.HLEN() != 100 {
		t.Errorf("HLEN=%d, want 100", h.HLEN())
	}
}

/* ---------- 全表扫描 ---------- */

func TestHKeysHValsHGetAll(t *testing.T) {
	h := NewRedisHash()
	input := map[string]string{
		"f1": "v1",
		"f2": "v2",
		"f3": "v3",
	}
	for f, v := range input {
		h.HSet(f, []byte(v))
	}

	// HKeys
	keys := h.HKeys()
	sort.Strings(keys)
	wantKeys := []string{"f1", "f2", "f3"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("HKeys=%v, want %v", keys, wantKeys)
	}

	// HVals
	vals := h.HVals()
	sort.Slice(vals, func(i, j int) bool { return bytes.Compare(vals[i], vals[j]) < 0 })
	wantVals := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	if !reflect.DeepEqual(vals, wantVals) {
		t.Errorf("HVals=%v, want %v", vals, wantVals)
	}

	// HGetAll
	all := h.HGetAll()
	if len(all) != 3 {
		t.Fatalf("HGetAll len=%d, want 3", len(all))
	}
	for f, v := range input {
		if !bytes.Equal(all[f], []byte(v)) {
			t.Errorf("HGetAll[%s]=%q, want %q", f, all[f], v)
		}
	}
}

/* ---------- 二进制安全 ---------- */

func TestBinarySafe(t *testing.T) {
	h := NewRedisHash()
	key := "bin"
	val := []byte("a\x00b\x00c")

	h.HSet(key, val)
	got, ok := h.HGet(key)
	if !ok || !bytes.Equal(got, val) {
		t.Error("binary safe HSet/HGet failed")
	}

	vals := h.HVals()
	if len(vals) != 1 || !bytes.Equal(vals[0], val) {
		t.Error("binary safe HVals failed")
	}

	m := h.HGetAll()
	if !bytes.Equal(m[key], val) {
		t.Error("binary safe HGetAll failed")
	}
}

/* ---------- 性能基准 ---------- */

func BenchmarkHSet(b *testing.B) {
	h := NewRedisHash()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.HSet(string(rune(i%10000)), []byte("val"))
	}
}

func BenchmarkHGet(b *testing.B) {
	h := NewRedisHash()
	h.HSet("key", []byte("val"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = h.HGet("key")
	}
}

func BenchmarkHGetAll(b *testing.B) {
	h := NewRedisHash()
	for i := 0; i < 1000; i++ {
		h.HSet(string(rune(i)), []byte("val"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.HGetAll()
	}
}
