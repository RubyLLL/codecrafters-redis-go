package main

import (
	"testing"
	"time"
)

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

func TestServerHandlesSetWithEX(t *testing.T) {
	server := newServer()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }

	if got := string(server.handleCommand([]string{"SET", "name", "redis", "EX", "10"})); got != "+OK\r\n" {
		t.Fatalf("SET EX response = %q, want +OK", got)
	}

	now = now.Add(9 * time.Second)
	if got := string(server.handleCommand([]string{"GET", "name"})); got != "$5\r\nredis\r\n" {
		t.Fatalf("GET before EX expiry = %q, want redis bulk string", got)
	}

	now = now.Add(time.Second)
	if got := string(server.handleCommand([]string{"GET", "name"})); got != "$-1\r\n" {
		t.Fatalf("GET after EX expiry = %q, want null bulk string", got)
	}
}

func TestServerHandlesSetWithPX(t *testing.T) {
	server := newServer()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }

	if got := string(server.handleCommand([]string{"SET", "name", "redis", "px", "500"})); got != "+OK\r\n" {
		t.Fatalf("SET PX response = %q, want +OK", got)
	}

	now = now.Add(499 * time.Millisecond)
	if got := string(server.handleCommand([]string{"GET", "name"})); got != "$5\r\nredis\r\n" {
		t.Fatalf("GET before PX expiry = %q, want redis bulk string", got)
	}

	now = now.Add(time.Millisecond)
	if got := string(server.handleCommand([]string{"GET", "name"})); got != "$-1\r\n" {
		t.Fatalf("GET after PX expiry = %q, want null bulk string", got)
	}
}

func TestServerRejectsInvalidSetExpiry(t *testing.T) {
	server := newServer()

	tests := [][]string{
		{"SET", "name", "redis", "EX", "0"},
		{"SET", "name", "redis", "PX", "-1"},
		{"SET", "name", "redis", "EX", "abc"},
		{"SET", "name", "redis", "XX", "10"},
	}

	for _, command := range tests {
		if got := string(server.handleCommand(command)); got == "+OK\r\n" {
			t.Fatalf("command %v succeeded unexpectedly", command)
		}
	}
}
