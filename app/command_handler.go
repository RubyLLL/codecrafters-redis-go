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
