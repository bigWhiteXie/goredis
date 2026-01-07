package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "goredis",
	Short: "A Redis-like server implemented in Go",
	Long:  "goredis is a Redis-compatible server implemented in Go for learning purposes.",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
