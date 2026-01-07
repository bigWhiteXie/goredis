package cmd

import (
	"fmt"
	"goredis/internal/server"

	"github.com/spf13/cobra"
)

var (
	addr   string
	aofDir string
	dbNum  int
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the Redis server",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := server.Config{
			Addr:   addr,
			AOFDir: aofDir,
			DBNum:  dbNum,
		}

		srv, err := server.NewServer(cfg)
		if err != nil {
			return err
		}

		fmt.Printf("goredis listening on %s\n", addr)
		return srv.ListenAndServe()
	},
}

func init() {
	runCmd.Flags().StringVar(&addr, "addr", ":6379", "server listen address")
	runCmd.Flags().StringVar(&aofDir, "aof-dir", "./data", "AOF persistence directory")
	runCmd.Flags().IntVar(&dbNum, "db-num", 16, "number of databases")

	rootCmd.AddCommand(runCmd)
}
