package main

import "testing"

func TestServerHandlesXaddExplicitID(t *testing.T) {
	server := newServer()

	if got := string(server.handleCommand([]string{"XADD", "mystream", "1526919030474-0", "temperature", "36", "humidity", "95"})); got != "$15\r\n1526919030474-0\r\n" {
		t.Fatalf("XADD response = %q, want inserted ID", got)
	}

	entry, ok := server.getLiveEntry("mystream")
	if !ok {
		t.Fatal("mystream key missing after XADD")
	}
	if entry.value.typ != streamValue {
		t.Fatalf("entry type = %v, want stream", entry.value.typ)
	}
	if len(entry.value.stream.entries) != 1 {
		t.Fatalf("stream entry count = %d, want 1", len(entry.value.stream.entries))
	}

	streamEntry := entry.value.stream.entries[0]
	if streamEntry.id.ms != 1526919030474 || streamEntry.id.seq != 0 {
		t.Fatalf("stream ID = %+v, want 1526919030474-0", streamEntry.id)
	}
	if len(streamEntry.fields) != 2 {
		t.Fatalf("field count = %d, want 2", len(streamEntry.fields))
	}
	if streamEntry.fields[0] != (streamField{key: "temperature", value: "36"}) {
		t.Fatalf("first field = %+v", streamEntry.fields[0])
	}
}

func TestServerRejectsXaddNonIncreasingID(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"XADD", "mystream", "1-1", "field", "value"})

	if got := string(server.handleCommand([]string{"XADD", "mystream", "1-1", "field", "value"})); got != "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n" {
		t.Fatalf("XADD duplicate ID response = %q", got)
	}
	if got := string(server.handleCommand([]string{"XADD", "mystream", "1-0", "field", "value"})); got != "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n" {
		t.Fatalf("XADD smaller ID response = %q", got)
	}
}

func TestServerRejectsInvalidXadd(t *testing.T) {
	server := newServer()

	tests := [][]string{
		{"XADD", "mystream", "0-0", "field", "value"},
		{"XADD", "mystream", "abc", "field", "value"},
		{"XADD", "mystream", "1-a", "field", "value"},
		{"XADD", "mystream", "1--1", "field", "value"},
		{"XADD", "mystream", "1-1", "field"},
	}

	for _, command := range tests {
		if got := string(server.handleCommand(command)); got == "$3\r\n1-1\r\n" || got == "+OK\r\n" {
			t.Fatalf("command %v succeeded unexpectedly with response %q", command, got)
		}
	}
}

func TestServerRejectsXaddWrongType(t *testing.T) {
	server := newServer()
	server.handleCommand([]string{"SET", "mystream", "value"})

	if got := string(server.handleCommand([]string{"XADD", "mystream", "1-0", "field", "value"})); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("XADD wrong type response = %q", got)
	}
}
