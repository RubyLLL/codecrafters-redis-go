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

	errEmptyCommand       = "ERR empty command"
	errUnknownCommand     = "ERR unknown command"
	errWrongType          = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errIntegerOutOfRange  = "ERR value is not an integer or out of range"
	errPositiveOutOfRange = "ERR value is out of range, must be positive"
	errInvalidExpireTime  = "ERR invalid expire time in 'set' command"
	errInvalidTimeout     = "ERR timeout is not a float or out of range"
	errSyntax             = "ERR syntax error"
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
	cond  *sync.Cond
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
	"LLEN":   (*server).handleLlen,
	"LPOP":   (*server).handleLpop,
	"BLPOP":  (*server).handleBlpop,
	"TYPE":   (*server).handleType,
}

func newServer() *server {
	s := &server{
		store: make(map[string]storeEntry),
		now:   time.Now,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
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
		return encodeSimpleError(errEmptyCommand)
	}

	name := strings.ToUpper(command[0])
	handler, ok := commandHandlers[name]
	if !ok {
		return encodeSimpleError(errUnknownCommand)
	}

	return handler(s, command[1:])
}

func (s *server) handlePing(args []string) []byte {
	if len(args) > 1 {
		return encodeWrongNumberOfArguments("ping")
	}
	if len(args) == 1 {
		return encodeBulkString(args[0])
	}

	return encodeSimpleString("PONG")
}

func (s *server) handleEcho(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("echo")
	}

	return encodeBulkString(args[0])
}

func (s *server) handleType(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("type")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeSimpleString("none")
	}

	switch entry.value.typ {
	case stringValue:
		return encodeSimpleString("string")
	case listValue:
		return encodeSimpleString("list")
	default:
		return encodeSimpleString("none")
	}
}

func (s *server) handleGet(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("get")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeNullBulkString()
	}

	if entry.value.typ != stringValue {
		return encodeSimpleError(errWrongType)
	}

	return encodeBulkString(entry.value.str)
}

func (s *server) handleSet(args []string) []byte {
	if len(args) != 2 && len(args) != 4 {
		return encodeWrongNumberOfArguments("set")
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
		return encodeWrongNumberOfArguments("rpush")
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
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	list = append(list, newValues...)
	entry.value = redisValue{typ: listValue, list: list}
	s.store[key] = entry
	s.cond.Broadcast()

	return encodeInteger(len(list))
}

func (s *server) handleLrange(args []string) []byte {
	if len(args) != 3 {
		return encodeWrongNumberOfArguments("lrange")
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return encodeSimpleError(errIntegerOutOfRange)
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeSimpleError(errIntegerOutOfRange)
	}

	entry, ok := s.getLiveEntry(key)

	if !ok {
		return encodeArray([]string{})
	}
	if entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
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
		return encodeWrongNumberOfArguments("lpush")
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
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	list = append(newValues, list...)
	entry.value = redisValue{typ: listValue, list: list}
	s.store[key] = entry
	s.cond.Broadcast()

	return encodeInteger(len(list))
}

func (s *server) handleLlen(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("llen")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeInteger(0)
	}

	if entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	return encodeInteger(len(entry.value.list))
}

func (s *server) handleLpop(args []string) []byte {
	if len(args) < 1 || len(args) > 2 {
		return encodeWrongNumberOfArguments("lpop")
	}

	key := args[0]
	deleteCount := 1
	hasCount := len(args) == 2
	if len(args) == 2 {
		var err error
		deleteCount, err = strconv.Atoi(args[1])
		if err != nil || deleteCount < 0 {
			return encodeSimpleError(errPositiveOutOfRange)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if !ok {
		if hasCount {
			return encodeNullArray()
		}
		return encodeNullBulkString()
	}
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
		if hasCount {
			return encodeNullArray()
		}
		return encodeNullBulkString()
	}
	if ok && entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	if len(list) == 0 {
		delete(s.store, key)
		if hasCount {
			return encodeArray([]string{})
		}
		return encodeNullBulkString()
	}
	if deleteCount == 0 {
		return encodeArray([]string{})
	}

	if deleteCount > len(list) {
		deleteCount = len(list)
	}

	popped := list[:deleteCount]
	remaining := list[deleteCount:]
	if len(remaining) == 0 {
		delete(s.store, key)
	} else {
		entry.value = redisValue{typ: listValue, list: remaining}
		s.store[key] = entry
	}

	if !hasCount {
		return encodeBulkString(popped[0])
	}

	return encodeArray(popped)
}

func (s *server) handleBlpop(args []string) []byte {
	if len(args) < 2 {
		return encodeWrongNumberOfArguments("blpop")
	}

	keys := args[:len(args)-1]
	timeout, err := strconv.ParseFloat(args[len(args)-1], 64)
	if err != nil || timeout < 0 {
		return encodeSimpleError(errInvalidTimeout)
	}

	deadline := time.Time{}
	if timeout > 0 {
		deadline = s.now().Add(time.Duration(timeout * float64(time.Second)))

		timer := time.AfterFunc(time.Until(deadline), func() {
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		})

		defer timer.Stop()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		for _, key := range keys {
			value, found, wrongType := s.popListHeadLocked(key)
			if wrongType {
				return encodeSimpleError(errWrongType)
			}
			if found {
				return encodeArray([]string{key, value})
			}
		}

		if !deadline.IsZero() && s.now().After(deadline) {
			return encodeNullArray()
		}

		s.cond.Wait()
	}
}

func (s *server) popListHeadLocked(key string) (string, bool, bool) {
	entry, ok := s.store[key]
	if !ok {
		return "", false, false
	}
	if entry.isExpired(s.now()) {
		delete(s.store, key)
		return "", false, false
	}
	if entry.value.typ != listValue {
		return "", false, true
	}

	list := entry.value.list
	if len(list) == 0 {
		delete(s.store, key)
		return "", false, false
	}

	value := list[0]
	remaining := list[1:]
	if len(remaining) == 0 {
		delete(s.store, key)
	} else {
		entry.value = redisValue{typ: listValue, list: remaining}
		s.store[key] = entry
	}

	return value, true, false
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
		return time.Time{}, errors.New(errInvalidExpireTime)
	}

	switch strings.ToUpper(option) {
	case "EX":
		return s.now().Add(time.Duration(value) * time.Second), nil
	case "PX":
		return s.now().Add(time.Duration(value) * time.Millisecond), nil
	default:
		return time.Time{}, errors.New(errSyntax)
	}
}

func (e storeEntry) isExpired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

func encodeWrongNumberOfArguments(command string) []byte {
	return encodeSimpleError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
}
