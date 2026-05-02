package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// what we actually receive is *1\r\n$4\r\nping\r\n
		line1, _ := reader.ReadString('\n') // *1\r\n
		line2, _ := reader.ReadString('\n') // $4\r\n
		line3, _ := reader.ReadString('\n') // PING\r\n

		fmt.Println(line1, line2, line3)

		if line3 == "PING\r\n" {
			_, err := conn.Write(encodeSimpleString("PONG"))
			if err != nil {
				return
			}
		}
	}
}

func encodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}
