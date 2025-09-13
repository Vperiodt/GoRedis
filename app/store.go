package main

import (
	"sync"
	"time"
)

type RedisList []string
type valueEntry struct {
	value   any
	expires time.Time
}

type StreamEntry struct {
	ID     string
	Fields []string // Changed from map[string]string to preserve order
}

type RedisStream struct {
	Entries []StreamEntry
}

var dataStore = struct {
	sync.RWMutex
	data map[string]valueEntry
}{
	data: make(map[string]valueEntry),
}
