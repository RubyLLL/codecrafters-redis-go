package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

const (
	OK string = "OK"
)

type server struct {
	store map[string]string
	mu    sync.RWMutex
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
		store: make(map[string]string),
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
	value, ok := s.store[args[0]]
	s.mu.RUnlock()

	if !ok {
		return encodeNullBulkString()
	}

	return encodeBulkString(value)
}

func (s *server) handleSet(args []string) []byte {
	if len(args) != 2 {
		return encodeSimpleError("ERR wrong number of arguments for 'set' command")
	}

	s.mu.Lock()
	s.store[args[0]] = args[1]
	s.mu.Unlock()

	return encodeSimpleString(OK)
}
