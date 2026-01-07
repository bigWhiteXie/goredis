package datastruct

import "goredis/internal/command"

func init() {
	command.RegisterCommand(&command.Command{
		Name:     "set",
		Arity:    -3,
		Executor: execSet,
	})
	command.RegisterCommand(&command.Command{
		Name:     "get",
		Arity:    2,
		Executor: execGet,
	})
}
