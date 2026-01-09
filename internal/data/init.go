package data

import "goredis/internal/command"

func init() {
	// ========================
	// String Commands
	// ========================

	command.RegisterCommand(&command.Command{
		Name:     "set",
		Arity:    -3, // set key value [options]
		Executor: execSet,
	})

	command.RegisterCommand(&command.Command{
		Name:     "get",
		Arity:    2, // get key
		Executor: execGet,
	})

	command.RegisterCommand(&command.Command{
		Name:     "setnx",
		Arity:    3, // setnx key value
		Executor: execSetNX,
	})

	command.RegisterCommand(&command.Command{
		Name:     "strlen",
		Arity:    2, // strlen key
		Executor: execStrLen,
	})

	command.RegisterCommand(&command.Command{
		Name:     "append",
		Arity:    3, // append key value
		Executor: execAppend,
	})

	command.RegisterCommand(&command.Command{
		Name:     "incr",
		Arity:    2, // incr key
		Executor: execIncr,
	})

	command.RegisterCommand(&command.Command{
		Name:     "decr",
		Arity:    2, // decr key
		Executor: execDecr,
	})

	command.RegisterCommand(&command.Command{
		Name:     "incrby",
		Arity:    3, // incrby key increment
		Executor: execIncrBy,
	})

	command.RegisterCommand(&command.Command{
		Name:     "decrby",
		Arity:    3, // decrby key decrement
		Executor: execDecrBy,
	})

	// ========================
	// List Commands
	// ========================
	command.RegisterCommand(&command.Command{
		Name:     "lpush",
		Arity:    -3, // lpush key element [element ...]
		Executor: execLPush,
	})

	command.RegisterCommand(&command.Command{
		Name:     "rpush",
		Arity:    -3, // rpush key element [element ...]
		Executor: execRPush,
	})

	command.RegisterCommand(&command.Command{
		Name:     "lpop",
		Arity:    2, // lpop key
		Executor: execLPop,
	})

	command.RegisterCommand(&command.Command{
		Name:     "rpop",
		Arity:    2, // rpop key
		Executor: execRPop,
	})

	command.RegisterCommand(&command.Command{
		Name:     "llen",
		Arity:    2, // llen key
		Executor: execLLen,
	})

	command.RegisterCommand(&command.Command{
		Name:     "lindex",
		Arity:    3, // lindex key index
		Executor: execLIndex,
	})

	command.RegisterCommand(&command.Command{
		Name:     "lset",
		Arity:    4, // lset key index element
		Executor: execLSet,
	})

	command.RegisterCommand(&command.Command{
		Name:     "lrange",
		Arity:    4, // lrange key start stop
		Executor: execLRange,
	})

	command.RegisterCommand(&command.Command{
		Name:     "lrem",
		Arity:    4, // lrem key count element
		Executor: execLRem,
	})

	command.RegisterCommand(&command.Command{
		Name:     "ltrim",
		Arity:    4, // ltrim key start stop
		Executor: execLTrim,
	})

	// ========================
	// Set Commands
	// ========================

	command.RegisterCommand(&command.Command{
		Name:     "sadd",
		Arity:    -3, // sadd key member [member ...]
		Executor: execSAdd,
	})

	command.RegisterCommand(&command.Command{
		Name:     "srem",
		Arity:    -3, // srem key member [member ...]
		Executor: execSRem,
	})

	command.RegisterCommand(&command.Command{
		Name:     "scard",
		Arity:    2, // scard key
		Executor: execSCard,
	})

	command.RegisterCommand(&command.Command{
		Name:     "smembers",
		Arity:    2, // smembers key
		Executor: execSMembers,
	})

	command.RegisterCommand(&command.Command{
		Name:     "sismember",
		Arity:    3, // sismember key member
		Executor: execSIsMember,
	})

	command.RegisterCommand(&command.Command{
		Name:     "spop",
		Arity:    -2, // spop key [count]
		Executor: execSPop,
	})

	command.RegisterCommand(&command.Command{
		Name:     "srandmember",
		Arity:    -2, // srandmember key [count]
		Executor: execSRandMember,
	})

	command.RegisterCommand(&command.Command{
		Name:     "zadd",
		Arity:    -4,
		Executor: execZAdd,
	})

	// ZCARD key
	command.RegisterCommand(&command.Command{
		Name:     "zcard",
		Arity:    2,
		Executor: execZCard,
	})

	// ZSCORE key member
	command.RegisterCommand(&command.Command{
		Name:     "zscore",
		Arity:    3,
		Executor: execZScore,
	})

	// ZRANK key member
	command.RegisterCommand(&command.Command{
		Name:     "zrank",
		Arity:    3,
		Executor: execZRank,
	})

	// ZREVRANK key member
	command.RegisterCommand(&command.Command{
		Name:     "zrevrank",
		Arity:    3,
		Executor: execZRevRank,
	})

	// ZRANGE key start stop [WITHSCORES]
	command.RegisterCommand(&command.Command{
		Name:     "zrange",
		Arity:    -4,
		Executor: execZRange,
	})

	// ZREVRANGE key start stop [WITHSCORES]
	command.RegisterCommand(&command.Command{
		Name:     "zrevrange",
		Arity:    -4,
		Executor: execZRevRange,
	})

	// ZCOUNT key min max
	command.RegisterCommand(&command.Command{
		Name:     "zcount",
		Arity:    4,
		Executor: execZCount,
	})

	// ZREM key member [member ...]
	command.RegisterCommand(&command.Command{
		Name:     "zrem",
		Arity:    -3,
		Executor: execZRem,
	})
}
