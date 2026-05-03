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

type server struct {
	store map[string]storeEntry
	mu    sync.RWMutex
	cond  *sync.Cond
	now   func() time.Time
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
