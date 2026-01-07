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
}
