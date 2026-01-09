package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"goredis/pkg/parser"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	cliAddr string
)

var cliCmd = &cobra.Command{
	Use:   "cli",
	Short: "Start a CLI client to connect to goredis server",
	RunE: func(cmd *cobra.Command, args []string) error {
		return startCLI(cliAddr)
	},
}

func init() {
	cliCmd.Flags().StringVar(&cliAddr, "addr", "127.0.0.1:6379", "server address to connect to")
	rootCmd.AddCommand(cliCmd)
}

// startCLI 启动命令行客户端
func startCLI(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	log.Printf("Connected to goredis at %s\n", addr)

	stdin := bufio.NewReader(os.Stdin)

	p := parser.NewParser(conn)

	for {
		fmt.Print("> ")
		line, err := stdin.ReadString('\n')
		if err != nil {
			fmt.Println("read input error:", err)
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			fmt.Println("bye")
			return nil
		}

		args := splitArgs(line)
		if len(args) == 0 {
			continue
		}

		req := encodeRESPArray(args)

		if _, err := conn.Write(req); err != nil {
			fmt.Println("write error:", err)
			return err
		}

		payload, err := p.Parse()
		if err != nil {
			fmt.Println("read response error:", err)
			return err
		}

		printRESP(payload)
	}
}

func encodeRESPArray(args []string) []byte {
	var buf bytes.Buffer

	// *<num>\r\n
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(len(args)))
	buf.WriteString("\r\n")

	for _, arg := range args {
		b := []byte(arg)

		// $<len>\r\n
		buf.WriteByte('$')
		buf.WriteString(strconv.Itoa(len(b)))
		buf.WriteString("\r\n")

		// <data>\r\n
		buf.Write(b)
		buf.WriteString("\r\n")
	}

	return buf.Bytes()
}

func splitArgs(line string) []string {
	fields := strings.Fields(line)
	return fields
}

func printRESP(v interface{}) {
	switch val := v.(type) {
	case nil:
		fmt.Println("(nil)")

	case []byte:
		fmt.Println(string(val))

	case string:
		fmt.Println(val)

	case int64:
		fmt.Println(val)

	case []interface{}:
		for _, e := range val {
			printRESP(e)
		}

	case error:
		fmt.Println("(error)", val.Error())

	default:
		fmt.Printf("%v\n", val)
	}
}
