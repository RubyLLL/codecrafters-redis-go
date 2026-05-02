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

func TestServerHandlesRpush(t *testing.T) {
	server := newServer()

	if got := string(server.handleCommand([]string{"RPUSH", "items", "a", "b"})); got != ":2\r\n" {
		t.Fatalf("first RPUSH response = %q, want length 2", got)
	}
	if got := string(server.handleCommand([]string{"RPUSH", "items", "c"})); got != ":3\r\n" {
		t.Fatalf("second RPUSH response = %q, want length 3", got)
	}

	entry, ok := server.getLiveEntry("items")
	if !ok {
		t.Fatal("items key missing after RPUSH")
	}
	want := []string{"a", "b", "c"}
	for i := range want {
		if entry.value.list[i] != want[i] {
			t.Fatalf("list[%d] = %q, want %q", i, entry.value.list[i], want[i])
		}
	}
}

func TestServerRejectsRpushOnStringKey(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"SET", "name", "redis"})

	if got := string(server.handleCommand([]string{"RPUSH", "name", "value"})); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("RPUSH string key response = %q", got)
	}
}

func TestServerRpushReplacesExpiredStringKey(t *testing.T) {
	server := newServer()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }

	server.handleCommand([]string{"SET", "items", "old", "PX", "500"})
	now = now.Add(500 * time.Millisecond)

	if got := string(server.handleCommand([]string{"RPUSH", "items", "new"})); got != ":1\r\n" {
		t.Fatalf("RPUSH expired string response = %q, want length 1", got)
	}

	entry, ok := server.getLiveEntry("items")
	if !ok {
		t.Fatal("items key missing after RPUSH")
	}
	if entry.value.typ != listValue || len(entry.value.list) != 1 || entry.value.list[0] != "new" {
		t.Fatalf("entry after RPUSH expired key = %+v", entry)
	}
}

func TestServerHandlesLrange(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"RPUSH", "items", "a", "b", "c", "d"})

	if got := string(server.handleCommand([]string{"LRANGE", "items", "1", "2"})); got != "*2\r\n$1\r\nb\r\n$1\r\nc\r\n" {
		t.Fatalf("LRANGE response = %q, want b and c", got)
	}
}

func TestServerHandlesLrangeNegativeIndexes(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"RPUSH", "items", "a", "b", "c", "d"})

	if got := string(server.handleCommand([]string{"LRANGE", "items", "-2", "-1"})); got != "*2\r\n$1\r\nc\r\n$1\r\nd\r\n" {
		t.Fatalf("LRANGE negative response = %q, want c and d", got)
	}
}

func TestServerHandlesEmptyLrange(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"RPUSH", "items", "a", "b"})

	tests := [][]string{
		{"LRANGE", "missing", "0", "-1"},
		{"LRANGE", "items", "10", "20"},
		{"LRANGE", "items", "1", "0"},
	}

	for _, command := range tests {
		if got := string(server.handleCommand(command)); got != "*0\r\n" {
			t.Fatalf("command %v response = %q, want empty array", command, got)
		}
	}
}

func TestServerRejectsInvalidLrange(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"SET", "name", "redis"})

	if got := string(server.handleCommand([]string{"LRANGE", "items", "start", "0"})); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("LRANGE invalid index response = %q", got)
	}
	if got := string(server.handleCommand([]string{"LRANGE", "name", "0", "-1"})); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("LRANGE string key response = %q", got)
	}
}
