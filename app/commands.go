package main

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func executeCommand(command string, args []string) []byte {
	switch command {
	case "PING":
		return []byte("+PONG\r\n")
	case "ECHO":
		return handleEcho(args)
	case "SET":
		return handleSet(args)
	case "GET":
		return handleGet(args)
	case "INCR":
		return handleIncr(args)
	case "TYPE":
		return handleType(args)
	case "XADD":
		return handleXadd(args)
	case "XRANGE":
		return handleXrange(args)
	case "LRANGE":
		return handleLrange(args)
	case "XREAD":
		return handleXread(args)
	case "CONFIG":
		return handleConfig(args)
	case "KEYS":
		return handleKeys(args)
	case "RPUSH":
		return handleRpush(args)
	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command))
	}
}
func parseFullStreamID(id string) (msTime int64, seqNum int64, err error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid stream ID format")
	}
	msTime, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	seqNum, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return msTime, seqNum, nil
}
func parseRangeID(idStr string, defaultSeq int64) (ms int64, seq int64, err error) {
	parts := strings.Split(idStr, "-")
	if len(parts) < 1 || len(parts) > 2 {
		return 0, 0, fmt.Errorf("invalid stream ID format")
	}

	ms, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid millisecond time part of stream ID")
	}

	if len(parts) == 1 {
		return ms, defaultSeq, nil
	}
	// len(parts) == 2
	seq, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sequence number part of stream ID")
	}
	return ms, seq, nil
}

func handleXrange(args []string) []byte {
	if len(args) != 3 {
		return []byte("-ERR wrong number of arguments for 'xrange' command\r\n")
	}
	key, startIDStr, endIDStr := args[0], args[1], args[2]

	dataStore.RLock()
	defer dataStore.RUnlock()

	entry, ok := dataStore.data[key]
	if !ok {
		return []byte("*0\r\n")
	}

	stream, isStream := entry.value.(*RedisStream)
	if !isStream {
		return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}

	var startMs, startSeq, endMs, endSeq int64
	var err error

	if startIDStr == "-" {
		startMs = 0
		startSeq = 0
	} else {
		startMs, startSeq, err = parseRangeID(startIDStr, 0)
		if err != nil {
			return []byte("-ERR invalid start ID for 'xrange' command\r\n")
		}
	}

	if endIDStr == "+" {
		endMs = math.MaxInt64
		endSeq = math.MaxInt64
	} else {
		endMs, endSeq, err = parseRangeID(endIDStr, math.MaxInt64)
		if err != nil {
			return []byte("-ERR invalid end ID for 'xrange' command\r\n")
		}
	}

	var results []StreamEntry
	for _, streamEntry := range stream.Entries {
		entryMs, entrySeq, _ := parseFullStreamID(streamEntry.ID)

		inStartRange := entryMs > startMs || (entryMs == startMs && entrySeq >= startSeq)
		inEndRange := entryMs < endMs || (entryMs == endMs && entrySeq <= endSeq)

		if inStartRange && inEndRange {
			results = append(results, streamEntry)
		}
	}

	// Build RESP response
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(results)))
	for _, resEntry := range results {
		builder.WriteString("*2\r\n")
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(resEntry.ID), resEntry.ID))

		builder.WriteString(fmt.Sprintf("*%d\r\n", len(resEntry.Fields)))
		for _, field := range resEntry.Fields {
			builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(field), field))
		}
	}
	return []byte(builder.String())
}

func handleEcho(args []string) []byte {
	if len(args) < 1 {
		return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
	return []byte(response)
}

func handleSet(args []string) []byte {
	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'set' command\r\n")
	}
	key, value := args[0], args[1]
	var expiry time.Time
	if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
		if expiryMillis, err := strconv.ParseInt(args[3], 10, 64); err == nil {
			expiry = time.Now().Add(time.Duration(expiryMillis) * time.Millisecond)
		} else {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
	}
	dataStore.Lock()
	defer dataStore.Unlock()
	dataStore.data[key] = valueEntry{value: value, expires: expiry}
	return []byte("+OK\r\n")
}

func handleGet(args []string) []byte {
	if len(args) < 1 {
		return []byte("-ERR wrong number of arguments for 'get' command\r\n")
	}
	key := args[0]
	dataStore.RLock()
	entry, ok := dataStore.data[key]
	dataStore.RUnlock()

	if !ok {
		return []byte("$-1\r\n")
	}

	if !entry.expires.IsZero() && time.Now().After(entry.expires) {
		dataStore.Lock()
		delete(dataStore.data, key)
		dataStore.Unlock()
		return []byte("$-1\r\n")
	}

	val, ok := entry.value.(string)
	if !ok {
		return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
}

func handleIncr(args []string) []byte {
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
	}

	currentValue, err := strconv.Atoi(entry.value.(string))
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	newValue := currentValue + 1
	entry.value = strconv.Itoa(newValue)
	dataStore.data[key] = entry
	return []byte(fmt.Sprintf(":%d\r\n", newValue))
}

func handleType(args []string) []byte {
	if len(args) < 1 {
		return []byte("-ERR wrong number of arguments for 'type' command\r\n")
	}
	key := args[0]
	dataStore.RLock()
	defer dataStore.RUnlock()

	entry, ok := dataStore.data[key]
	if !ok || (!entry.expires.IsZero() && time.Now().After(entry.expires)) {
		return []byte("+none\r\n")
	}

	switch entry.value.(type) {
	case string:
		return []byte("+string\r\n")
	case *RedisStream:
		return []byte("+stream\r\n")
	case RedisList:
		return []byte("+list\r\n")
	default:
		return []byte("+none\r\n")
	}
}

func handleXadd(args []string) []byte {
	if len(args) < 3 || (len(args)-2)%2 != 0 {
		return []byte("-ERR wrong number of arguments for 'xadd' command\r\n")
	}
	key, idArg := args[0], args[1]

	dataStore.Lock()
	defer dataStore.Unlock()

	var stream *RedisStream
	if entry, ok := dataStore.data[key]; ok {
		var isStream bool
		stream, isStream = entry.value.(*RedisStream)
		if !isStream {
			return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		}
	} else {
		stream = &RedisStream{}
		dataStore.data[key] = valueEntry{value: stream}
	}

	var newID string
	if idArg == "*" {
		now := time.Now().UnixMilli()
		seq := int64(0)
		if len(stream.Entries) > 0 {
			lastID := stream.Entries[len(stream.Entries)-1].ID
			parts := strings.Split(lastID, "-")
			if lastTime, err := strconv.ParseInt(parts[0], 10, 64); err == nil && lastTime == now {
				lastSeq, _ := strconv.ParseInt(parts[1], 10, 64)
				seq = lastSeq + 1
			}
		}
		newID = fmt.Sprintf("%d-%d", now, seq)
	} else if strings.HasSuffix(idArg, "-*") {
		timePartStr := strings.TrimSuffix(idArg, "-*")
		timePart, err := strconv.ParseInt(timePartStr, 10, 64)
		if err != nil {
			return []byte("-ERR Invalid stream ID format\r\n")
		}

		seq := int64(0)
		if len(stream.Entries) > 0 {
			lastID := stream.Entries[len(stream.Entries)-1].ID
			lastParts := strings.Split(lastID, "-")
			if lastTime, err := strconv.ParseInt(lastParts[0], 10, 64); err == nil && lastTime == timePart {
				lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)
				seq = lastSeq + 1
			}
		} else if timePart == 0 {
			seq = 1
		}
		newID = fmt.Sprintf("%d-%d", timePart, seq)

	} else {
		parts := strings.Split(idArg, "-")
		if len(parts) != 2 {
			return []byte("-ERR Invalid stream ID format. It must be in the form <ms>-<seq>.\r\n")
		}
		ms, errMs := strconv.ParseInt(parts[0], 10, 64)
		seq, errSeq := strconv.ParseInt(parts[1], 10, 64)
		if errMs != nil || errSeq != nil {
			return []byte("-ERR Invalid stream ID format. It must be in the form <ms>-<seq>.\r\n")
		}

		if ms == 0 && seq == 0 {
			return []byte("-ERR The ID specified in XADD must be greater than 0-0\r\n")
		}

		if len(stream.Entries) > 0 {
			lastID := stream.Entries[len(stream.Entries)-1].ID
			lastParts := strings.Split(lastID, "-")
			lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
			lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)
			if ms < lastMs || (ms == lastMs && seq <= lastSeq) {
				return []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
			}
		}
		newID = idArg
	}

	// Store fields as a slice
	streamEntry := StreamEntry{
		ID:     newID,
		Fields: args[2:],
	}
	stream.Entries = append(stream.Entries, streamEntry)
	notificationManager.Notify(key)
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(newID), newID))
}

func handleConfig(args []string) []byte {
	if len(args) < 2 || strings.ToUpper(args[0]) != "GET" {
		return []byte("-ERR Syntax error for CONFIG command\r\n")
	}
	paramName := args[1]
	var paramValue string

	switch strings.ToLower(paramName) {
	case "dir":
		paramValue = serverConfig.Dir
	case "dbfilename":
		paramValue = serverConfig.DbFileName
	default:
		return []byte("*0\r\n")
	}
	return []byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(paramName), paramName, len(paramValue), paramValue))
}

func handleKeys(args []string) []byte {
	if len(args) < 1 || args[0] != "*" {
		return []byte("-ERR unsupported KEYS pattern\r\n")
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
	return []byte(response)
}

func handleXread(args []string) []byte {
	var blockTimeoutMs int64 = -1
	streamsArgIndex := 0

	if len(args) > 1 && strings.ToLower(args[0]) == "block" {
		if len(args) < 3 {
			return []byte("-ERR syntax error for 'block' option\r\n")
		}
		var err error
		blockTimeoutMs, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		streamsArgIndex = 2
	}

	if len(args) <= streamsArgIndex || strings.ToLower(args[streamsArgIndex]) != "streams" {
		return []byte("-ERR Syntax error, try XREAD [BLOCK ms] STREAMS key [key ...]\r\n")
	}

	remainingArgs := args[streamsArgIndex+1:]
	numStreams := len(remainingArgs) / 2
	if len(remainingArgs) < 2 || len(remainingArgs)%2 != 0 {
		return []byte("-ERR Unbalanced XREAD list of streams: keys and IDs must match\r\n")
	}

	keys := remainingArgs[:numStreams]
	ids := remainingArgs[numStreams:]

	resolvedIDs := make([]string, numStreams)
	for i, id := range ids {
		if id == "$" {
			key := keys[i]
			dataStore.RLock()
			entry, ok := dataStore.data[key]
			if ok {
				stream, isStream := entry.value.(*RedisStream)
				if isStream && len(stream.Entries) > 0 {
					resolvedIDs[i] = stream.Entries[len(stream.Entries)-1].ID
				} else {
					resolvedIDs[i] = "0-0"
				}
			} else {
				resolvedIDs[i] = "0-0"
			}
			dataStore.RUnlock()
		} else {
			resolvedIDs[i] = id
		}
	}
	initialResult := performNonBlockingRead(keys, resolvedIDs)
	if initialResult != nil {
		return initialResult
	}

	if blockTimeoutMs < 0 {
		return []byte("*0\r\n")
	}

	var timeoutChan <-chan time.Time
	if blockTimeoutMs > 0 {
		timeoutChan = time.After(time.Duration(blockTimeoutMs) * time.Millisecond)
	}
	cases := make([]reflect.SelectCase, len(keys)+1)
	for i, key := range keys {
		ch := notificationManager.GetChan(key)
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	cases[len(keys)] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timeoutChan)}

	chosen, _, _ := reflect.Select(cases)

	if chosen == len(keys) && timeoutChan != nil {
		return []byte("*-1\r\n")
	}
	finalResult := performNonBlockingRead(keys, resolvedIDs)
	if finalResult != nil {
		return finalResult
	}

	if blockTimeoutMs > 0 {
		return []byte("*-1\r\n")
	}

	return []byte("*0\r\n")
}

func performNonBlockingRead(keys []string, ids []string) []byte {
	dataStore.RLock()
	defer dataStore.RUnlock()

	var streamResults []string

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		startIDStr := ids[i]

		startMs, startSeq, err := parseFullStreamID(startIDStr)
		if err != nil {
			continue
		}

		entry, ok := dataStore.data[key]
		if !ok {
			continue
		}
		stream, isStream := entry.value.(*RedisStream)
		if !isStream {
			continue
		}

		var foundEntries []StreamEntry
		for _, streamEntry := range stream.Entries {
			entryMs, entrySeq, _ := parseFullStreamID(streamEntry.ID)
			if entryMs > startMs || (entryMs == startMs && entrySeq > startSeq) {
				foundEntries = append(foundEntries, streamEntry)
			}
		}

		if len(foundEntries) > 0 {
			var streamResultBuilder strings.Builder
			streamResultBuilder.WriteString("*2\r\n")
			streamResultBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))
			streamResultBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(foundEntries)))
			for _, foundEntry := range foundEntries {
				streamResultBuilder.WriteString("*2\r\n")
				streamResultBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(foundEntry.ID), foundEntry.ID))
				streamResultBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(foundEntry.Fields)))
				for _, field := range foundEntry.Fields {
					streamResultBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(field), field))
				}
			}
			streamResults = append(streamResults, streamResultBuilder.String())
		}
	}

	if len(streamResults) == 0 {
		return nil
	}

	var responseBuilder strings.Builder
	responseBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(streamResults)))
	for _, result := range streamResults {
		responseBuilder.WriteString(result)
	}
	return []byte(responseBuilder.String())
}

func handleRpush(args []string) []byte {
	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'rpush' command\r\n")
	}
	key := args[0]
	elements := args[1:]

	dataStore.Lock()
	defer dataStore.Unlock()

	var list RedisList
	entry, ok := dataStore.data[key]

	if ok {
		existingList, isList := entry.value.(RedisList)
		if !isList {
			return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		}
		list = existingList
	} else {
		list = make(RedisList, 0)
	}

	list = append(list, elements...)

	dataStore.data[key] = valueEntry{value: list}

	return []byte(fmt.Sprintf(":%d\r\n", len(list)))
}
func handleLrange(args []string) []byte {
	if len(args) != 3 {
		return []byte("-ERR wrong number of arguments for 'lrange' command\r\n")
	}
	key := args[0]
	startStr := args[1]
	endStr := args[2]

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	end, err := strconv.Atoi(endStr)
	if err != nil {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	dataStore.RLock()
	defer dataStore.RUnlock()

	entry, ok := dataStore.data[key]
	if !ok {
		return []byte("*0\r\n")
	}

	list, isList := entry.value.(RedisList)
	if !isList {
		return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}

	listLen := len(list)

	if start < 0 {
		start = listLen + start
	}
	if end < 0 {
		end = listLen + end
	}

	if start < 0 {
		start = 0
	}

	if start >= listLen || start > end {
		return []byte("*0\r\n")
	}

	if end >= listLen {
		end = listLen - 1
	}
	subList := list[start : end+1]

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(subList)))
	for _, item := range subList {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
	}

	return []byte(builder.String())
}
