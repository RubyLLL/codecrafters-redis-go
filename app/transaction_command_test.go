package main

import "testing"

func TestTransactionStateIsPerClient(t *testing.T) {
	server := newServer()
	client1 := &clientSession{server: server}
	client2 := &clientSession{server: server}

	if got := string(client1.handleCommand([]string{"MULTI"})); got != "+OK\r\n" {
		t.Fatalf("MULTI response = %q, want OK", got)
	}
	if got := string(client1.handleCommand([]string{"SET", "orange", "32"})); got != "+QUEUED\r\n" {
		t.Fatalf("queued SET response = %q, want QUEUED", got)
	}
	if got := string(client1.handleCommand([]string{"INCR", "orange"})); got != "+QUEUED\r\n" {
		t.Fatalf("queued INCR response = %q, want QUEUED", got)
	}

	if got := string(client2.handleCommand([]string{"GET", "orange"})); got != "$-1\r\n" {
		t.Fatalf("GET from another client during transaction = %q, want null bulk string", got)
	}

	want := "*2\r\n+OK\r\n:33\r\n"
	if got := string(client1.handleCommand([]string{"EXEC"})); got != want {
		t.Fatalf("EXEC response = %q, want %q", got, want)
	}
	if got := string(client2.handleCommand([]string{"GET", "orange"})); got != "$2\r\n33\r\n" {
		t.Fatalf("GET after EXEC = %q, want 33", got)
	}
}
