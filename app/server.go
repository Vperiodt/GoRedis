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
)

type NotificationManager struct {
	mu       sync.Mutex
	keyChans map[string]chan struct{}
}

func (nm *NotificationManager) GetChan(key string) <-chan struct{} {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	ch, ok := nm.keyChans[key]
	if !ok {
		ch = make(chan struct{})
		nm.keyChans[key] = ch
	}
	return ch
}

func (nm *NotificationManager) Notify(key string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if ch, ok := nm.keyChans[key]; ok {
		close(ch)
		delete(nm.keyChans, key)
	}
}

var notificationManager = &NotificationManager{
	keyChans: make(map[string]chan struct{}),
}
var serverConfig struct {
	Dir        string
	DbFileName string
}

type QueuedCommand struct {
	Command string
	Args    []string
}

type Server struct {
	listener net.Listener
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start() error {
	flag.StringVar(&serverConfig.Dir, "dir", ".", "The directory where RDB files are stored")
	flag.StringVar(&serverConfig.DbFileName, "dbfilename", "dump.rdb", "The name of the RDB file")
	flag.Parse()

	if err := loadRdbFile(); err != nil && !os.IsNotExist(err) {
		fmt.Println("Error loading RDB file:", err)
	}

	fmt.Println("Logs from your program will appear here!")

	var err error
	s.listener, err = net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		return fmt.Errorf("failed to bind to port 6379: %w", err)
	}
	defer s.listener.Close()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Transaction state is specific to each connection
	inTransaction := false
	var commandQueue []QueuedCommand

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading command array:", err.Error())
			}
			return
		}
		if line[0] != '*' {
			continue
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
			data := make([]byte, length+2)
			if _, err := io.ReadFull(reader, data); err != nil {
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

		if inTransaction && command != "EXEC" && command != "DISCARD" {
			commandQueue = append(commandQueue, QueuedCommand{Command: command, Args: cmdArgs})
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		switch command {
		case "MULTI":
			if inTransaction {
				conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
			} else {
				inTransaction = true
				commandQueue = nil
				conn.Write([]byte("+OK\r\n"))
			}
		case "EXEC":
			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			} else {
				response := fmt.Sprintf("*%d\r\n", len(commandQueue))
				conn.Write([]byte(response))
				for _, cmd := range commandQueue {
					cmdResponse := executeCommand(cmd.Command, cmd.Args)
					conn.Write(cmdResponse)
				}
				inTransaction = false
				commandQueue = nil
			}
		case "DISCARD":
			if !inTransaction {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			} else {
				inTransaction = false
				commandQueue = nil
				conn.Write([]byte("+OK\r\n"))
			}
		default:
			response := executeCommand(command, cmdArgs)
			conn.Write(response)
		}
	}
}
