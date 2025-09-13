package main

import (
	"fmt"
	"os"
)

func main() {
	server := NewServer()
	if err := server.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
