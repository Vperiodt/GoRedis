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
	Port             string
	Role             string
	MasterReplID     string
	MasterReplOffset int
	Dir              string
	DbFileName       string
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
	flag.StringVar(&serverConfig.Port, "port", "6379", "The port to listen on")

	var replicaOf string
	flag.StringVar(&replicaOf, "replicaof", "", "The master server to replicate from, e.g., 'localhost 6379'")

	flag.StringVar(&serverConfig.Dir, "dir", ".", "The directory where RDB files are stored")
	flag.StringVar(&serverConfig.DbFileName, "dbfilename", "dump.rdb", "The name of the RDB file")
	flag.Parse()

	if replicaOf != "" {
		serverConfig.Role = "slave"
		parts := strings.Split(replicaOf, " ")
		if len(parts) == 2 {
			masterHost := parts[0]
			masterPort := parts[1]
			go initiateHandshake(masterHost, masterPort)
		}
	} else {
		serverConfig.Role = "master"

		serverConfig.MasterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		serverConfig.MasterReplOffset = 0
	}

	if err := loadRdbFile(); err != nil && !os.IsNotExist(err) {
		fmt.Println("Error loading RDB file:", err)
	}

	fmt.Println("Logs from your program will appear here!")

	var err error
	listenAddr := fmt.Sprintf("0.0.0.0:%s", serverConfig.Port)
	s.listener, err = net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %w", serverConfig.Port, err)
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

type ListWaiter struct {
	key    string
	signal chan struct{}
}
type BlockingListManager struct {
	mu      sync.Mutex
	waiters map[string][]*ListWaiter
}

func (blm *BlockingListManager) GetWaiter(key string) *ListWaiter {
	blm.mu.Lock()
	defer blm.mu.Unlock()

	waiter := &ListWaiter{
		key:    key,
		signal: make(chan struct{}, 1),
	}
	blm.waiters[key] = append(blm.waiters[key], waiter)
	return waiter
}

func (blm *BlockingListManager) RemoveWaiter(waiter *ListWaiter) {
	blm.mu.Lock()
	defer blm.mu.Unlock()

	queue, ok := blm.waiters[waiter.key]
	if !ok {
		return
	}
	newQueue := make([]*ListWaiter, 0)
	for _, w := range queue {
		if w != waiter {
			newQueue = append(newQueue, w)
		}
	}
	blm.waiters[waiter.key] = newQueue
}

func (blm *BlockingListManager) Notify(key string) {
	blm.mu.Lock()
	defer blm.mu.Unlock()

	queue, ok := blm.waiters[key]
	if !ok || len(queue) == 0 {
		return
	}

	waiter := queue[0]
	blm.waiters[key] = queue[1:]
	waiter.signal <- struct{}{}
}

var blockingListManager = &BlockingListManager{
	waiters: make(map[string][]*ListWaiter),
}

func initiateHandshake(masterHost, masterPort string) {
	masterAddr := fmt.Sprintf("%s:%s", masterHost, masterPort)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		os.Exit(1)
	}

	reader := bufio.NewReader(conn)

	pingCmd := "*1\r\n$4\r\nPING\r\n"
	_, err = conn.Write([]byte(pingCmd))
	if err != nil {
		fmt.Println("Failed to send PING to master:", err)
		return
	}
	response, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(response) != "+PONG" {
		fmt.Println("Invalid response to PING from master:", response)
		return
	}
	fmt.Println("Received PONG from master")

	replconfPortCmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(serverConfig.Port), serverConfig.Port)
	_, err = conn.Write([]byte(replconfPortCmd))
	if err != nil {
		fmt.Println("Failed to send REPLCONF port to master:", err)
		return
	}
	response, err = reader.ReadString('\n')
	if err != nil || strings.TrimSpace(response) != "+OK" {
		fmt.Println("Invalid response to REPLCONF port from master:", response)
		return
	}
	fmt.Println("Received OK for REPLCONF listening-port")

	replconfCapaCmd := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	_, err = conn.Write([]byte(replconfCapaCmd))
	if err != nil {
		fmt.Println("Failed to send REPLCONF capa to master:", err)
		return
	}
	response, err = reader.ReadString('\n')
	if err != nil || strings.TrimSpace(response) != "+OK" {
		fmt.Println("Invalid response to REPLCONF capa from master:", response)
		return
	}
	fmt.Println("Received OK for REPLCONF capa")
	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err = conn.Write([]byte(psyncCmd))
	if err != nil {
		fmt.Println("Failed to send PSYNC to master:", err)
		return
	}
	response, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read PSYNC response from master:", err)
		return
	}
	fmt.Println("Received response for PSYNC:", strings.TrimSpace(response))
}
