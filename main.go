package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var _ = net.Listen
var _ = os.Exit

var serverConfig struct {
	Dir        string
	DbFileName string
}

type valueEntry struct {
	value   string
	expires time.Time
}

var dataStore = struct {
	sync.RWMutex
	data map[string]valueEntry
}{
	data: make(map[string]valueEntry),
}

func main() {

	flag.StringVar(&serverConfig.Dir, "dir", ".", "The directory where RDB files are stored")
	flag.StringVar(&serverConfig.DbFileName, "dbfilename", "dump.rdb", "The name of the RDB file")
	flag.Parse()

	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}

}
func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading command array:", err.Error())
			}
			return
		}

		numArgs, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			fmt.Println("Invalid number of arguments:", err.Error())
			return
		}
		var args []string
		for i := 0; i < numArgs; i++ {

			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading bulk string length:", err.Error())
				return
			}

			if line[0] != '$' {
				fmt.Println("Invalid bulk string format")
				return
			}

			length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
			if err != nil {
				fmt.Println("Invalid bulk string length:", err.Error())
				return
			}
			data := make([]byte, length+2) // +2 for \r\n
			_, err = io.ReadFull(reader, data)
			if err != nil {
				fmt.Println("Error reading bulk string data:", err.Error())
				return
			}

			args = append(args, string(data[:length]))
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
			conn.Write([]byte(response))
		case "SET":
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			key := args[1]
			value := args[2]

			var expiry time.Time
			if len(args) > 4 && strings.ToUpper(args[3]) == "PX" {
				expiryMillis, err := strconv.ParseInt(args[4], 10, 64)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}
				expiry = time.Now().Add(time.Duration(expiryMillis) * time.Millisecond)
			}

			dataStore.Lock()
			dataStore.data[key] = valueEntry{
				value:   value,
				expires: expiry,
			}
			dataStore.Unlock()
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			key := args[1]

			var response []byte

			dataStore.Lock()

			entry, ok := dataStore.data[key]

			if !ok {
				response = []byte("$-1\r\n")

			} else if !entry.expires.IsZero() && time.Now().After(entry.expires) {
				delete(dataStore.data, key)
				response = []byte("$-1\r\n")

			} else {
				response = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.value), entry.value))
			}

			dataStore.Unlock()

			conn.Write(response)
		case "CONFIG":
			if len(args) < 3 || strings.ToUpper(args[1]) != "GET" {
				conn.Write([]byte("-ERR Syntax error for CONFIG command\r\n"))
				continue
			}
			paramName := args[2]

			var paramValue string

			switch strings.ToLower(paramName) {
			case "dir":
				paramValue = serverConfig.Dir
			case "dbfilename":
				paramValue = serverConfig.DbFileName
			default:
				continue
			}
			response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(paramName), paramName, len(paramValue), paramValue)
			conn.Write([]byte(response))

		default:
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))

		}
	}
}
