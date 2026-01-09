package data

import (
	"bytes"
	"goredis/pkg/datastruct"
	"sort"
	"strconv"
)

// ZSetInterface 抽象 Redis ZSet 命令操作
type ZSetInterface interface {
	// ZADD key [NX|XX] [CH] score member [score member ...]
	ZAdd(ch bool, nx bool, xx bool, score float64, member []byte) int

	// ZREM key member [member ...]
	ZRem(member []byte) int

	// ZSCORE key member
	ZScore(member []byte) (float64, bool)

	// ZRANK key member (0-based)
	ZRank(member []byte) (int, bool)

	// ZREVRANK key member
	ZRevRank(member []byte) (int, bool)

	// ZRANGE key start stop [WITHSCORES]
	ZRange(start, stop int, withScores bool) [][]byte

	// ZREVRANGE key start stop [WITHSCORES]
	ZRevRange(start, stop int, withScores bool) [][]byte

	// ZCOUNT key min max
	ZCount(min, max float64) int

	// ZINCRBY key increment member
	ZIncrBy(delta float64, member []byte) float64

	// ZCARD key
	ZCard() int

	// Clear 清空 ZSet
	Clear()
}

var _ ZSetInterface = &ZSet{}

const (
	zsetMaxZiplist = 128 // 小规模使用 listpack
)

type ZSet struct {
	dict map[string]float64   // member -> score 映射
	sl   *datastruct.SkipList // 大规模用 SkipList
	lp   *datastruct.ListPack // 小规模用 listpack，元素存成 [score|member]
}

func NewZSet() *ZSet {
	return &ZSet{
		dict: make(map[string]float64),
		lp:   datastruct.NewListPack(zsetMaxZiplist),
		sl:   nil,
	}
}

func (zs *ZSet) ZAdd(ch bool, nx bool, xx bool, score float64, member []byte) int {
	memberStr := string(member)
	added := 0

	// 小规模使用 listpack
	if zs.sl == nil && zs.lp.Len() < zsetMaxZiplist {
		_, exists := zs.dict[memberStr]

		// NX/XX 语义判断
		if nx && exists {
			return 0
		}
		if xx && !exists {
			return 0
		}

		if exists {
			// 删除原来的 member
			for i := 0; i < zs.lp.Len(); i++ {
				entry := zs.lp.GetRaw(i)
				if entry != nil {
					_, oldMember := decodeZSetEntry(entry)
					if bytes.Equal(oldMember, member) {
						zs.lp.RemoveAt(i)
						break
					}
				}
			}
		} else {
			added = 1
		}

		// 插入新的 entry
		zs.lp.PushBack(encodeZSetEntry(score, member))
		zs.dict[memberStr] = score

		// 超过阈值升级
		if zs.lp.Len() > zsetMaxZiplist {
			zs.upgradeToSkipList()
		}

		return added
	}

	// 大规模使用 SkipList + dict
	if zs.sl == nil {
		zs.upgradeToSkipList()
	}

	oldScore, exists := zs.dict[memberStr]
	if nx && exists {
		return 0
	}
	if xx && !exists {
		return 0
	}

	if exists {
		zs.sl.Delete(oldScore, member)
	} else {
		added = 1
	}

	zs.sl.Insert(score, member)
	zs.dict[memberStr] = score
	return added
}

// 升级 listpack -> SkipList
func (zs *ZSet) upgradeToSkipList() {
	zs.sl = datastruct.NewSkipList()
	for i := 0; i < zs.lp.Len(); i++ {
		entry := zs.lp.GetRaw(i)
		score, member := decodeZSetEntry(entry)
		zs.sl.Insert(score, member)
	}
	zs.lp = nil
}

func (zs *ZSet) ZRem(member []byte) int {
	memberStr := string(member)
	oldScore, exists := zs.dict[memberStr]
	if !exists {
		return 0
	}

	delete(zs.dict, memberStr)

	if zs.sl != nil {
		zs.sl.Delete(oldScore, member)
	} else {
		// 小规模 listpack
		for i := 0; i < zs.lp.Len(); i++ {
			entry := zs.lp.GetRaw(i)
			_, m := decodeZSetEntry(entry)
			if bytes.Equal(m, member) {
				zs.lp.RemoveAt(i)
				break
			}
		}
	}
	return 1
}

func (zs *ZSet) ZScore(member []byte) (float64, bool) {
	score, ok := zs.dict[string(member)]
	return score, ok
}

func (zs *ZSet) ZRank(member []byte) (int, bool) {
	score, ok := zs.dict[string(member)]
	if !ok {
		return -1, false
	}

	if zs.sl != nil {
		r := zs.sl.GetRank(score, member)
		return r, r >= 0
	}

	// listpack 遍历
	for i := 0; i < zs.lp.Len(); i++ {
		entry := zs.lp.GetRaw(i)
		s, m := decodeZSetEntry(entry)
		if bytes.Equal(m, member) {
			rank := 0
			// 计算小于 score 的个数
			for j := 0; j < zs.lp.Len(); j++ {
				e := zs.lp.GetRaw(j)
				s2, _ := decodeZSetEntry(e)
				if s2 < s {
					rank++
				}
			}
			return rank, true
		}
	}

	return -1, false
}

func (zs *ZSet) ZRevRank(member []byte) (int, bool) {
	memberStr := string(member)

	if zs.sl != nil {
		// 大规模 skiplist 场景
		score, ok := zs.dict[memberStr]
		if !ok {
			return -1, false
		}
		rank := zs.sl.GetRank(score, member) // 正序 rank
		if rank < 0 {
			return -1, false
		}
		revRank := zs.sl.Len() - rank - 1
		return revRank, true
	}

	// 小规模 listpack 场景
	type entry struct {
		score  float64
		member []byte
	}
	entries := make([]entry, zs.lp.Len())
	for i := 0; i < zs.lp.Len(); i++ {
		raw := zs.lp.GetRaw(i)
		score, mem := decodeZSetEntry(raw)
		entries[i] = entry{score, mem}
	}

	// 升序排序
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score == entries[j].score {
			return string(entries[i].member) < string(entries[j].member)
		}
		return entries[i].score < entries[j].score
	})

	// 逆序查找
	for i := len(entries) - 1; i >= 0; i-- {
		if bytes.Equal(entries[i].member, member) {
			return len(entries) - 1 - i, true
		}
	}

	return -1, false
}

func (zs *ZSet) ZRange(start, stop int, withScores bool) [][]byte {
	if zs.sl != nil {
		return zs.sl.RangeToBytes(start, stop, withScores)
	}

	// 小规模 listpack
	type entry struct {
		score  float64
		member []byte
	}

	var entries []entry
	for i := 0; i < zs.lp.Len(); i++ {
		raw := zs.lp.GetRaw(i)
		score, member := decodeZSetEntry(raw)
		entries = append(entries, entry{score, member})
	}

	// 按 score 升序排序
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score == entries[j].score {
			return string(entries[i].member) < string(entries[j].member)
		}
		return entries[i].score < entries[j].score
	})

	// slice 范围处理
	if start < 0 {
		start = len(entries) + start
	}
	if stop < 0 {
		stop = len(entries) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(entries) {
		stop = len(entries) - 1
	}
	if start > stop || len(entries) == 0 {
		return nil
	}

	var res [][]byte
	for i := start; i <= stop; i++ {
		res = append(res, entries[i].member)
		if withScores {
			res = append(res, []byte(strconv.FormatFloat(entries[i].score, 'f', -1, 64)))
		}
	}

	return res
}

func (zs *ZSet) ZRevRange(start, stop int, withScores bool) [][]byte {
	if zs.sl != nil {
		return zs.sl.ReverseRangeToBytes(start, stop, withScores)
	}

	// 小规模 listpack
	type entry struct {
		score  float64
		member []byte
	}

	var entries []entry
	for i := 0; i < zs.lp.Len(); i++ {
		raw := zs.lp.GetRaw(i)
		score, member := decodeZSetEntry(raw)
		entries = append(entries, entry{score, member})
	}

	// 升序排序
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score == entries[j].score {
			return string(entries[i].member) < string(entries[j].member)
		}
		return entries[i].score < entries[j].score
	})

	// 逆序处理
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	// slice 范围
	if start < 0 {
		start = len(entries) + start
	}
	if stop < 0 {
		stop = len(entries) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(entries) {
		stop = len(entries) - 1
	}
	if start > stop || len(entries) == 0 {
		return nil
	}

	var res [][]byte
	for i := start; i <= stop; i++ {
		res = append(res, entries[i].member)
		if withScores {
			res = append(res, []byte(strconv.FormatFloat(entries[i].score, 'f', -1, 64)))
		}
	}

	return res
}

func (zs *ZSet) ZCard() int {
	return len(zs.dict)
}

func (zs *ZSet) ZIncrBy(delta float64, member []byte) float64 {
	oldScore, exists := zs.dict[string(member)]
	newScore := oldScore + delta
	if exists {
		zs.ZAdd(false, false, true, newScore, member)
	} else {
		zs.ZAdd(false, false, false, newScore, member)
	}
	return newScore
}

func (zs *ZSet) ZCount(min, max float64) int {
	count := 0

	// 小规模 ListPack
	if zs.sl == nil {
		for i := 0; i < zs.lp.Len(); i++ {
			entry := zs.lp.GetRaw(i)
			if entry == nil {
				continue
			}
			score, _ := decodeZSetEntry(entry)
			if score >= min && score <= max {
				count++
			}
		}
		return count
	}

	// 大规模 SkipList
	nodes := zs.sl.RangeByScore(min, max, true)

	return len(nodes)
}

func (zs *ZSet) Clear() {
	zs.dict = make(map[string]float64)
	zs.lp = datastruct.NewListPack(zsetMaxZiplist)
	zs.sl = nil
}

func encodeZSetEntry(score float64, member []byte) []byte {
	return []byte(strconv.FormatFloat(score, 'f', -1, 64) + ":" + string(member))
}

func decodeZSetEntry(entry []byte) (float64, []byte) {
	parts := bytes.SplitN(entry, []byte(":"), 2)
	score, _ := strconv.ParseFloat(string(parts[0]), 64)
	return score, parts[1]
}
