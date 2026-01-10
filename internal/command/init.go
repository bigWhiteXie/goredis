package command

func GetCmd(name string) (Command, bool) {
	cmd, ok := cmdTable[name]
	return *cmd, ok
}

func init() {
	RegisterCommand(&Command{
		Name:     "del",
		Arity:    -2,
		Executor: execDel,
	})
	RegisterCommand(&Command{
		Name:     "expire",
		Arity:    3,
		Executor: execExpire,
	})

	// ========================
	// String Commands
	// ========================
	RegisterCommand(&Command{
		Name:     "set",
		Arity:    -3, // set key value [options]
		Executor: execSet,
	})

	RegisterCommand(&Command{
		Name:     "get",
		Arity:    2, // get key
		Executor: execGet,
	})

	RegisterCommand(&Command{
		Name:     "setnx",
		Arity:    3, // setnx key value
		Executor: execSetNX,
	})

	RegisterCommand(&Command{
		Name:     "strlen",
		Arity:    2, // strlen key
		Executor: execStrLen,
	})

	RegisterCommand(&Command{
		Name:     "append",
		Arity:    3, // append key value
		Executor: execAppend,
	})

	RegisterCommand(&Command{
		Name:     "incr",
		Arity:    2, // incr key
		Executor: execIncr,
	})

	RegisterCommand(&Command{
		Name:     "decr",
		Arity:    2, // decr key
		Executor: execDecr,
	})

	RegisterCommand(&Command{
		Name:     "incrby",
		Arity:    3, // incrby key increment
		Executor: execIncrBy,
	})

	RegisterCommand(&Command{
		Name:     "decrby",
		Arity:    3, // decrby key decrement
		Executor: execDecrBy,
	})

	RegisterCommand(&Command{
		Name:     "mget",
		Arity:    -2, // decrby key decrement
		Executor: execMGet,
	})
	RegisterCommand(&Command{
		Name:     "mset",
		Arity:    -3, // decrby key decrement
		Executor: execMSet,
	})

	// ========================
	// List Commands
	// ========================
	RegisterCommand(&Command{
		Name:     "lpush",
		Arity:    -3, // lpush key element [element ...]
		Executor: execLPush,
	})

	RegisterCommand(&Command{
		Name:     "rpush",
		Arity:    -3, // rpush key element [element ...]
		Executor: execRPush,
	})

	RegisterCommand(&Command{
		Name:     "lpop",
		Arity:    2, // lpop key
		Executor: execLPop,
	})

	RegisterCommand(&Command{
		Name:     "rpop",
		Arity:    2, // rpop key
		Executor: execRPop,
	})

	RegisterCommand(&Command{
		Name:     "llen",
		Arity:    2, // llen key
		Executor: execLLen,
	})

	RegisterCommand(&Command{
		Name:     "lindex",
		Arity:    3, // lindex key index
		Executor: execLIndex,
	})

	RegisterCommand(&Command{
		Name:     "lset",
		Arity:    4, // lset key index element
		Executor: execLSet,
	})

	RegisterCommand(&Command{
		Name:     "lrange",
		Arity:    4, // lrange key start stop
		Executor: execLRange,
	})

	RegisterCommand(&Command{
		Name:     "lrem",
		Arity:    4, // lrem key count element
		Executor: execLRem,
	})

	RegisterCommand(&Command{
		Name:     "ltrim",
		Arity:    4, // ltrim key start stop
		Executor: execLTrim,
	})

	// ========================
	// Set Commands
	// ========================

	RegisterCommand(&Command{
		Name:     "sadd",
		Arity:    -3, // sadd key member [member ...]
		Executor: execSAdd,
	})

	RegisterCommand(&Command{
		Name:     "srem",
		Arity:    -3, // srem key member [member ...]
		Executor: execSRem,
	})

	RegisterCommand(&Command{
		Name:     "scard",
		Arity:    2, // scard key
		Executor: execSCard,
	})

	RegisterCommand(&Command{
		Name:     "smembers",
		Arity:    2, // smembers key
		Executor: execSMembers,
	})

	RegisterCommand(&Command{
		Name:     "sismember",
		Arity:    3, // sismember key member
		Executor: execSIsMember,
	})

	RegisterCommand(&Command{
		Name:     "spop",
		Arity:    -2, // spop key [count]
		Executor: execSPop,
	})

	RegisterCommand(&Command{
		Name:     "srandmember",
		Arity:    -2, // srandmember key [count]
		Executor: execSRandMember,
	})
	RegisterCommand(&Command{
		Name:     "sunion",
		Arity:    -3,
		Executor: execSUnion,
	})
	RegisterCommand(&Command{
		Name:     "sinter",
		Arity:    -3,
		Executor: execSInter,
	})

	// ========================
	// ZSet Commands
	// ========================
	RegisterCommand(&Command{
		Name:     "zadd",
		Arity:    -4,
		Executor: execZAdd,
	})

	// ZCARD key
	RegisterCommand(&Command{
		Name:     "zcard",
		Arity:    2,
		Executor: execZCard,
	})

	// ZSCORE key member
	RegisterCommand(&Command{
		Name:     "zscore",
		Arity:    3,
		Executor: execZScore,
	})

	// ZRANK key member
	RegisterCommand(&Command{
		Name:     "zrank",
		Arity:    3,
		Executor: execZRank,
	})

	// ZREVRANK key member
	RegisterCommand(&Command{
		Name:     "zrevrank",
		Arity:    3,
		Executor: execZRevRank,
	})

	// ZRANGE key start stop [WITHSCORES]
	RegisterCommand(&Command{
		Name:     "zrange",
		Arity:    -4,
		Executor: execZRange,
	})

	// ZREVRANGE key start stop [WITHSCORES]
	RegisterCommand(&Command{
		Name:     "zrevrange",
		Arity:    -4,
		Executor: execZRevRange,
	})

	// ZCOUNT key min max
	RegisterCommand(&Command{
		Name:     "zcount",
		Arity:    4,
		Executor: execZCount,
	})

	// ZREM key member [member ...]
	RegisterCommand(&Command{
		Name:     "zrem",
		Arity:    -3,
		Executor: execZRem,
	})

	// ========================
	// HashMap Commands
	// ========================
	RegisterCommand(&Command{
		Name:     "hset",
		Arity:    4,
		Executor: execHSet,
	})
	RegisterCommand(&Command{
		Name:     "hget",
		Arity:    3,
		Executor: execHGet,
	})
	RegisterCommand(&Command{
		Name:     "hdel",
		Arity:    -3,
		Executor: execHDel,
	})
	RegisterCommand(&Command{
		Name:     "hexists",
		Arity:    3,
		Executor: execHExists,
	})
	RegisterCommand(&Command{
		Name:     "hlen",
		Arity:    2,
		Executor: execHLEN,
	})
	RegisterCommand(&Command{
		Name:     "hkeys",
		Arity:    2,
		Executor: execHKeys,
	})
	RegisterCommand(&Command{
		Name:     "hvals",
		Arity:    2,
		Executor: execHVals,
	})
	RegisterCommand(&Command{
		Name:     "hgetall",
		Arity:    2,
		Executor: execHGetAll,
	})
	RegisterCommand(&Command{
		Name:     "hmset",
		Arity:    -4,
		Executor: execHMSet,
	})
	RegisterCommand(&Command{
		Name:     "hmget",
		Arity:    -3,
		Executor: execHMGet,
	})
}
