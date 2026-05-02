package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	OK string = "OK"
)

type valueType int

const (
	stringValue valueType = iota
	listValue
)

type redisValue struct {
	typ  valueType
	str  string
	list []string
}

type server struct {
	store map[string]storeEntry
	mu    sync.RWMutex
	now   func() time.Time
}

type storeEntry struct {
	value     redisValue
	expiresAt time.Time
}

type commandHandler func(*server, []string) []byte

var commandHandlers = map[string]commandHandler{
	"PING":   (*server).handlePing,
	"ECHO":   (*server).handleEcho,
	"GET":    (*server).handleGet,
	"SET":    (*server).handleSet,
	"RPUSH":  (*server).handleRpush,
	"LRANGE": (*server).handleLrange,
	"LPUSH":  (*server).handleLpush,
}

func newServer() *server {
	return &server{
		store: make(map[string]storeEntry),
		now:   time.Now,
	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("Error reading command: ", err.Error())
			}
			return
		}

		response := s.handleCommand(command)
		if _, err := conn.Write(response); err != nil {
			return
		}
	}
}

func (s *server) handleCommand(command []string) []byte {
	if len(command) == 0 {
		return encodeSimpleError("ERR empty command")
	}

	name := strings.ToUpper(command[0])
	handler, ok := commandHandlers[name]
	if !ok {
		return encodeSimpleError("ERR unknown command")
	}

	return handler(s, command[1:])
}

func (s *server) handlePing(args []string) []byte {
	if len(args) > 1 {
		return encodeSimpleError("ERR wrong number of arguments for 'ping' command")
	}
	if len(args) == 1 {
		return encodeBulkString(args[0])
	}

	return encodeSimpleString("PONG")
}

func (s *server) handleEcho(args []string) []byte {
	if len(args) != 1 {
		return encodeSimpleError("ERR wrong number of arguments for 'echo' command")
	}

	return encodeBulkString(args[0])
}

func (s *server) handleGet(args []string) []byte {
	if len(args) != 1 {
		return encodeSimpleError("ERR wrong number of arguments for 'get' command")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeNullBulkString()
	}

	if entry.value.typ != stringValue {
		return encodeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return encodeBulkString(entry.value.str)
}

func (s *server) handleSet(args []string) []byte {
	if len(args) != 2 && len(args) != 4 {
		return encodeSimpleError("ERR wrong number of arguments for 'set' command")
	}

	v := redisValue{
		typ: stringValue,
		str: args[1],
	}
	entry := storeEntry{value: v}
	if len(args) == 4 {
		expiresAt, err := s.parseSetExpiry(args[2], args[3])
		if err != nil {
			return encodeSimpleError(err.Error())
		}
		entry.expiresAt = expiresAt
	}

	s.mu.Lock()
	s.store[args[0]] = entry
	s.mu.Unlock()

	return encodeSimpleString(OK)
}

func (s *server) handleRpush(args []string) []byte {
	if len(args) < 2 {
		return encodeSimpleError("ERR wrong number of arguments for 'rpush' command")
	}

	key := args[0]
	newValues := args[1:]

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
	}
	if ok && entry.value.typ != listValue {
		return encodeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := entry.value.list
	list = append(list, newValues...)
	entry.value = redisValue{typ: listValue, list: list}
	s.store[key] = entry

	return encodeInteger(len(list))
}

func (s *server) handleLrange(args []string) []byte {
	if len(args) != 3 {
		return encodeSimpleError("ERR wrong number of arguments for 'lrange' command")
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return encodeSimpleError("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeSimpleError("ERR value is not an integer or out of range")
	}

	entry, ok := s.getLiveEntry(key)

	if !ok {
		return encodeArray([]string{})
	}
	if entry.value.typ != listValue {
		return encodeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := entry.value.list
	start, end, ok = normalizeListRange(start, end, len(list))
	if !ok {
		return encodeArray([]string{})
	}

	return encodeArray(list[start : end+1])
}

func (s *server) handleLpush(args []string) []byte {
	if len(args) < 2 {
		return encodeSimpleError("ERR wrong number of arguments for 'rpush' command")
	}

	key := args[0]
	newValues := args[1:]

	// reverse array
	for i, j := 0, len(newValues)-1; i < j; i, j = i+1, j-1 {
		newValues[i], newValues[j] = newValues[j], newValues[i]
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
	}
	if ok && entry.value.typ != listValue {
		return encodeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := entry.value.list
	list = append(newValues, list...)
	entry.value = redisValue{typ: listValue, list: list}
	s.store[key] = entry

	return encodeInteger(len(list))
}

func normalizeListRange(start int, end int, length int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}

	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}

	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start >= length || start > end {
		return 0, 0, false
	}

	return start, end, true
}

func (s *server) getLiveEntry(key string) (storeEntry, bool) {
	s.mu.RLock()
	entry, ok := s.store[key]
	if !ok || !entry.isExpired(s.now()) {
		s.mu.RUnlock()
		return entry, ok
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-read after taking the write lock: another goroutine may have replaced
	// this key while we were switching from the read lock to the write lock.
	entry, ok = s.store[key]
	if !ok {
		return storeEntry{}, false
	}
	if entry.isExpired(s.now()) {
		delete(s.store, key)
		return storeEntry{}, false
	}

	return entry, true
}

func (s *server) parseSetExpiry(option string, rawValue string) (time.Time, error) {
	value, err := strconv.Atoi(rawValue)
	if err != nil || value <= 0 {
		return time.Time{}, fmt.Errorf("ERR invalid expire time in 'set' command")
	}

	switch strings.ToUpper(option) {
	case "EX":
		return s.now().Add(time.Duration(value) * time.Second), nil
	case "PX":
		return s.now().Add(time.Duration(value) * time.Millisecond), nil
	default:
		return time.Time{}, fmt.Errorf("ERR syntax error")
	}
}

func (e storeEntry) isExpired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}
