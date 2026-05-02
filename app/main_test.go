package main

import (
	"bufio"
	"strings"
	"testing"
)

func TestReadRESPCommandParsesBulkStringArray(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n"))

	command, err := readRESPCommand(reader)
	if err != nil {
		t.Fatalf("readRESPCommand returned error: %v", err)
	}

	want := []string{"ECHO", "hello world"}
	if len(command) != len(want) {
		t.Fatalf("command length = %d, want %d", len(command), len(want))
	}

	for i := range want {
		if command[i] != want[i] {
			t.Fatalf("command[%d] = %q, want %q", i, command[i], want[i])
		}
	}
}

func TestReadRESPCommandConsumesOnePipelinedFrame(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"))

	first, err := readRESPCommand(reader)
	if err != nil {
		t.Fatalf("first readRESPCommand returned error: %v", err)
	}
	second, err := readRESPCommand(reader)
	if err != nil {
		t.Fatalf("second readRESPCommand returned error: %v", err)
	}

	if first[0] != "PING" || second[0] != "PING" {
		t.Fatalf("commands = %q and %q, want both PING", first[0], second[0])
	}
}

func TestReadRESPCommandRejectsMalformedBulkTerminator(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*1\r\n$4\r\nPING\n"))

	if _, err := readRESPCommand(reader); err == nil {
		t.Fatal("readRESPCommand returned nil error for malformed bulk terminator")
	}
}
