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

		switch command {
		case "MULTI":
			if inTransaction {
				conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
			} else {
				inTransaction = true
				commandQueue = nil // Reset queue
				conn.Write([]byte("+OK\r\n"))
			}
		case "EXEC":
			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(commandQueue))))
				for _, cmd := range commandQueue {
					cmdResponse := executeCommand(conn, cmd.Command, cmd.Args)
					conn.Write(cmdResponse)
					if serverConfig.Role == "master" {
						fullCmdArgs := append([]string{cmd.Command}, cmd.Args...)
						replicationState.Propagate(fullCmdArgs)
					}
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
			if inTransaction {
				commandQueue = append(commandQueue, QueuedCommand{Command: command, Args: cmdArgs})
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				response := executeCommand(conn, command, cmdArgs)

				if serverConfig.Role == "master" {
					switch command {
					case "SET", "RPUSH", "LPUSH", "LPOP", "INCR", "XADD":
						replicationState.Propagate(args)
					}
				}

				if response != nil {
					conn.Write(response)
				}
			}
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
	defer conn.Close()

	reader := bufio.NewReader(conn)
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	reader.ReadString('\n')
	replconfPortCmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(serverConfig.Port), serverConfig.Port)
	conn.Write([]byte(replconfPortCmd))
	reader.ReadString('\n')
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	reader.ReadString('\n')
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	reader.ReadString('\n')

	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading RDB file length:", err)
		return
	}
	if line[0] == '$' {
		length, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		buf := make([]byte, length)
		io.ReadFull(reader, buf)
	}

	fmt.Println("Handshake and RDB sync completed. Listening for commands.")

	var replicaOffset int = 0
	for {
		var totalCommandBytes int = 0

		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading command from master:", err.Error())
			}
			return
		}
		totalCommandBytes += len(line)

		if line[0] != '*' {
			continue
		}
		numArgs, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			continue
		}

		var args []string
		for i := 0; i < numArgs; i++ {

			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			totalCommandBytes += len(line)

			length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
			if err != nil {
				continue
			}

			data := make([]byte, length+2)
			bytesRead, err := io.ReadFull(reader, data)
			if err != nil {
				return
			}
			totalCommandBytes += bytesRead

			args = append(args, string(data[:length]))
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		cmdArgs := args[1:]

		if command == "REPLCONF" && len(cmdArgs) >= 2 && strings.ToLower(cmdArgs[0]) == "getack" {
			ackResponse := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.Itoa(replicaOffset)), replicaOffset)
			conn.Write([]byte(ackResponse))
		} else {

			executeCommand(nil, command, cmdArgs)
		}

		replicaOffset += totalCommandBytes
	}
}

type ReplicationState struct {
	mu       sync.Mutex
	replicas []net.Conn
}

func (rs *ReplicationState) AddReplica(conn net.Conn) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.replicas = append(rs.replicas, conn)
}

func (rs *ReplicationState) Propagate(commandArgs []string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(commandArgs)))
	for _, arg := range commandArgs {
		commandBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	cmd := []byte(commandBuilder.String())

	for _, replicaConn := range rs.replicas {
		if replicaConn != nil {
			replicaConn.Write(cmd)
		}
	}
}

var replicationState = &ReplicationState{}

func (rs *ReplicationState) GetReplicaCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.replicas)
}
