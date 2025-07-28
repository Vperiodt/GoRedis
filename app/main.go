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

// configuration values for your server
var serverConfig struct {
	Dir        string
	DbFileName string
}

// value associated with a key in data
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

	err := loadRdbFile()
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("Error loading RDB file:", err)
		}
	}
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

// type to store a command to be queued

type QueuedCommand struct {
	Command string
	Args    []string
}

func executeCommand(command string, args []string) []byte {

	switch command {
	case "PING":
		return []byte("+PONG\r\n")
	case "ECHO":
		if len(args) < 1 {
			return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
		}
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
		return []byte(response)
	case "SET":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'set' command\r\n")
		}
		key := args[0]
		value := args[1]

		var expiry time.Time
		if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
			expiryMillis, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return []byte("-ERR value is not an integer or out of range\r\n")
			}
			expiry = time.Now().Add(time.Duration(expiryMillis) * time.Millisecond)
		}

		dataStore.Lock()
		defer dataStore.Unlock()
		dataStore.data[key] = valueEntry{
			value:   value,
			expires: expiry,
		}
		return []byte("+OK\r\n")

	case "GET":
		if len(args) < 1 {
			return []byte("-ERR wrong number of arguments for 'get' command\r\n")
		}
		key := args[0]

		dataStore.RLock() // Acquire a read lock first

		entry, ok := dataStore.data[key]

		//  Key doesn't exist at all
		if !ok {
			dataStore.RUnlock() // Release the read lock
			return []byte("$-1\r\n")
		}

		//  Key exists, check if it's expired
		if !entry.expires.IsZero() && time.Now().After(entry.expires) {
			dataStore.RUnlock() // Release the read lock so we can get a write lock

			// Now get a write lock to safely delete the key
			dataStore.Lock()
			entry, ok = dataStore.data[key]
			if ok && !entry.expires.IsZero() && time.Now().After(entry.expires) {
				delete(dataStore.data, key)
			}
			dataStore.Unlock() // Release the write lock
			return []byte("$-1\r\n")
		}

		response := fmt.Sprintf("$%d\r\n%s\r\n", len(entry.value), entry.value)
		dataStore.RUnlock() // Release the read lock
		return []byte(response)

	case "INCR":
		if len(args) < 1 {
			return []byte("-ERR wrong number of arguments for 'incr' command\r\n")
		}
		key := args[0]
		dataStore.Lock()
		defer dataStore.Unlock()
		entry, ok := dataStore.data[key]
		if !ok {
			newValue := 1
			dataStore.data[key] = valueEntry{value: strconv.Itoa(newValue)}
			return []byte(fmt.Sprintf(":%d\r\n", newValue))
		} else {
			currentValue, err := strconv.Atoi(entry.value)
			if err != nil {
				return []byte("-ERR value is not an integer or out of range\r\n")
			} else {
				newValue := currentValue + 1
				entry.value = strconv.Itoa(newValue)
				dataStore.data[key] = entry
				return []byte(fmt.Sprintf(":%d\r\n", newValue))
			}
		}
	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command))
	}
}
func handleConnection(conn net.Conn) {
	defer conn.Close()

	inTransactions := false
	var commandQueue []QueuedCommand

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
		cmdArgs := args[1:]
		// If we are in a transaction, queue commands instead of executing them.
		if inTransactions && command != "MULTI" && command != "EXEC" && command != "DISCARD" && command != "WATCH" {
			cmd := QueuedCommand{
				Command: command,
				Args:    args[1:],
			}
			commandQueue = append(commandQueue, cmd)
			conn.Write([]byte("+QUEUED\r\n"))
			continue

		}

		switch command {

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
		case "KEYS":
			if len(args) < 2 || args[1] != "*" {
				conn.Write([]byte("-ERR unsupported KEYS pattern\r\n"))
				continue
			}

			dataStore.RLock()
			keys := make([]string, 0, len(dataStore.data))
			for k := range dataStore.data {
				keys = append(keys, k)
			}
			dataStore.RUnlock()

			response := fmt.Sprintf("*%d\r\n", len(keys))
			for _, k := range keys {
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
			}
			conn.Write([]byte(response))

		case "MULTI":
			inTransactions = true
			conn.Write([]byte("+OK\r\n"))
		case "EXEC":
			if !inTransactions {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			} else {

				responses := make([][]byte, len(commandQueue))

				for i, cmd := range commandQueue {
					responses[i] = executeCommand(cmd.Command, cmd.Args)
				}

				var finalResponse strings.Builder
				finalResponse.WriteString(fmt.Sprintf("*%d\r\n", len(responses)))
				for _, res := range responses {
					finalResponse.Write(res)
				}

				conn.Write([]byte(finalResponse.String()))

				inTransactions = false
				commandQueue = nil
			}
		case "DISCARD":
			if !inTransactions {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			} else {
				//reset the transaction state and clear the queue
				inTransactions = false
				commandQueue = nil
				conn.Write([]byte("+OK\r\n"))
			}
		default:
			response := executeCommand(command, cmdArgs)
			conn.Write(response)

		}
	}
}
