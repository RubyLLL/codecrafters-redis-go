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

type server struct {
	store map[string]storeEntry
	mu    sync.RWMutex
	now   func() time.Time
}

type storeEntry struct {
	value     string
	expiresAt time.Time
}

type commandHandler func(*server, []string) []byte

var commandHandlers = map[string]commandHandler{
	"PING": (*server).handlePing,
	"ECHO": (*server).handleEcho,
	"GET":  (*server).handleGet,
	"SET":  (*server).handleSet,
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

	s.mu.RLock()
	entry, ok := s.store[args[0]]
	s.mu.RUnlock()

	if !ok {
		return encodeNullBulkString()
	}
	if entry.isExpired(s.now()) {
		s.mu.Lock()
		if current, ok := s.store[args[0]]; ok && current.isExpired(s.now()) {
			delete(s.store, args[0])
		}
		s.mu.Unlock()

		return encodeNullBulkString()
	}

	return encodeBulkString(entry.value)
}

func (s *server) handleSet(args []string) []byte {
	if len(args) != 2 && len(args) != 4 {
		return encodeSimpleError("ERR wrong number of arguments for 'set' command")
	}

	entry := storeEntry{value: args[1]}
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
