package datastruct

import (
	"sync"
	"sync/atomic"

	"golang.org/x/exp/rand"
)

const DefaultDictSize = 1024

// ShardCount 分片数量，建议为 2 的幂次，方便位运算取模
// 数量越多，锁粒度越小，并发度越高，但内存消耗稍大
const ShardCount = 1024

// Consumer 用于遍历的回调，返回 false 则停止遍历
type Consumer func(key string, data interface{}) bool

// Dict 抽象接口，屏蔽底层实现细节
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)         // 对应 Redis SET
	PutIfAbsent(key string, val interface{}) (result int) // 对应 Redis SETNX
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int) // 对应 Redis DEL
	Keys() []string                 // 对应 Redis KEYS *
	ForEach(consumer Consumer)      // 遍历所有数据
	RandomKeys(limit int) []string  // 对应 Redis RANDOMKEY，用于驱逐策略
	Clear()                         // 清空
}

// shard 单个分片结构
type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex // 每个分片一把锁
}

// ConcurrentDict 全局并发 Map
type ConcurrentDict struct {
	table      []*shard // 分片切片
	count      int32    // 全局数据量统计 (使用原子操作)
	shardCount int      // 分片数 (主要用于取模)
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shards := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	return &ConcurrentDict{
		table:      shards,
		count:      0,
		shardCount: shardCount,
	}
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	shard := dict.getShard(key)
	shard.mutex.RLock() // 只加读锁
	defer shard.mutex.RUnlock()
	val, exists = shard.m[key]
	return
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	shard := dict.getShard(key)
	shard.mutex.Lock() // 加写锁
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0 // 覆盖
	}
	shard.m[key] = val
	atomic.AddInt32(&dict.count, 1) // 原子增加总数
	return 1                        // 新增
}

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	shard := dict.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		return 0 // 存在，不操作
	}
	shard.m[key] = val
	atomic.AddInt32(&dict.count, 1)
	return 1
}

func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	shard := dict.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1 // 更新成功
	}
	return 0 // 不存在，不更新
}

func (dict *ConcurrentDict) Remove(key string) (result int) {
	shard := dict.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		atomic.AddInt32(&dict.count, -1) // 原子减少
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Clear() {
	for _, shard := range dict.table {
		shard.mutex.Lock()
		shard.m = make(map[string]interface{})
		shard.mutex.Unlock()
	}
	atomic.StoreInt32(&dict.count, 0)
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		return
	}
	// 逐个分片遍历，防止长时间锁死整个 DB
	for _, shard := range dict.table {
		shard.mutex.RLock() // 锁住当前分片
		// 这里的 func 定义：如果在遍历中 consumer 返回 false，是否中断？
		// 通常 Redis 的 KEYS 是全量遍历，但 SCAN 是分批。
		// 这里简单实现为全量遍历
		continueIter := true
		for key, value := range shard.m {
			continueIter = consumer(key, value)
			if !continueIter {
				break
			}
		}
		shard.mutex.RUnlock()
		if !continueIter {
			break
		}
	}
}

func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, 0, dict.Len())
	dict.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	used := make(map[string]bool)
	result := make([]string, limit)
	// 随机选分片，再从分片里随机选 key
	// 注意：Go 的 map 遍历本身就是随机的，但我们需要跨分片随机

	for i := 0; i < limit; i++ {
		flag := false
		for {
			shard := dict.table[rand.Intn(dict.shardCount)]
			if len(shard.m) == 0 {
				continue
			}

			shard.mutex.RLock()
			// Go map 的随机性：range 一个就可以拿到随机元素
			for key := range shard.m {
				if !used[key] {
					result[i] = key
					used[key] = true
					flag = true
					break // 拿到一个就跑
				}
			}
			shard.mutex.RUnlock()
			if flag {
				break
			}
		}
	}
	return result
}

func (dict *ConcurrentDict) Len() int {
	return int(atomic.LoadInt32(&dict.count))
}

// getShard 根据 key 定位分片
func (dict *ConcurrentDict) getShard(key string) *shard {
	hash := computeHash(key)
	shardIdx := hash % uint32(dict.shardCount)

	return dict.table[shardIdx]
}

func computeHash(key string) uint32 {
	const prime32 = 16777619
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
