package main

import "strings"

type commandHandler func(args []string) []byte

var commandHandlers = map[string]commandHandler{
	"PING": handlePing,
	"ECHO": handleEcho,
}

func handleCommand(command []string) []byte {
	if len(command) == 0 {
		return encodeSimpleError("empty command")
	}

	name := strings.ToUpper(command[0])
	handler, ok := commandHandlers[name]
	if !ok {
		return encodeSimpleError("unknown command")
	}

	return handler(command[1:])
}

func handlePing(args []string) []byte {
	return encodeSimpleString("PONG")
}

func handleEcho(args []string) []byte {
	return encodeBulkString(args[0])
}
