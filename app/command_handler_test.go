package main

import "testing"

func TestServerHandlesSetThenGet(t *testing.T) {
	server := newServer()

	if got := string(server.handleCommand([]string{"SET", "name", "redis"})); got != "+OK\r\n" {
		t.Fatalf("SET response = %q, want +OK", got)
	}

	if got := string(server.handleCommand([]string{"GET", "name"})); got != "$5\r\nredis\r\n" {
		t.Fatalf("GET response = %q, want redis bulk string", got)
	}
}

func TestServerHandlesMissingGet(t *testing.T) {
	server := newServer()

	if got := string(server.handleCommand([]string{"GET", "missing"})); got != "$-1\r\n" {
		t.Fatalf("GET missing response = %q, want null bulk string", got)
	}
}

func TestServerRejectsWrongArity(t *testing.T) {
	server := newServer()

	if got := string(server.handleCommand([]string{"SET", "key"})); got != "-ERR wrong number of arguments for 'set' command\r\n" {
		t.Fatalf("SET wrong arity response = %q", got)
	}
}
