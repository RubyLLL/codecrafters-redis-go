package main

import "strings"

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
	"XADD":   (*server).handleXadd,
	"XRANGE": (*server).handleXrange,
	"XREAD":  (*server).handleXread,
	"INCR":   (*server).handleIncr,
}

func (s *server) handleCommand(command []string) []byte {
	if len(command) == 0 {
		return encodeSimpleError(errEmptyCommand)
	}

	name := strings.ToUpper(command[0])

	switch name {
	case "MULTI":
		return s.handleMulti(command[1:])
	case "EXEC":
		return s.handleExec(command[1:])
	case "GET":
		return s.handleGet(command[1:])
	}

	if s.transactional {
		s.queue = append(s.queue, append([]string(nil), command...))
		return encodeSimpleString(QUEUED)
	}

	return s.executeCommand(command)
}

func (s *server) executeCommand(command []string) []byte {
	name := strings.ToUpper(command[0])
	handler, ok := commandHandlers[name]
	if !ok {
		return encodeSimpleError(errUnknownCommand)
	}
	return handler(s, command[1:])
}
