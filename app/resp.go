package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	value, err := readRESPValue(reader)
	if err != nil {
		return nil, err
	}

	array, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected RESP array command, got %T", value)
	}

	command := make([]string, 0, len(array))
	for _, item := range array {
		switch typed := item.(type) {
		case string:
			command = append(command, typed)
		case int:
			command = append(command, strconv.Itoa(typed))
		case nil:
			command = append(command, "")
		default:
			return nil, fmt.Errorf("unsupported command argument type %T", item)
		}
	}

	return command, nil
}

// reference can be found at https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description
func readRESPValue(reader *bufio.Reader) (interface{}, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {

	// simple strings
	case '+', '-':
		return readRESPLine(reader)
	// integers
	case ':':
		line, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}
		return strconv.Atoi(line)
	// bulk strings
	case '$':
		line, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length %q: %w", line, err)
		}
		if length == -1 {
			return nil, nil
		}
		if length < -1 {
			return nil, fmt.Errorf("invalid bulk string length %d", length)
		}

		data := make([]byte, length+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		if data[length] != '\r' || data[length+1] != '\n' {
			return nil, fmt.Errorf("bulk string missing CRLF terminator")
		}

		return string(data[:length]), nil
	// arrays
	case '*':
		line, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid array length %q: %w", line, err)
		}
		if length == -1 {
			return nil, nil
		}
		if length < -1 {
			return nil, fmt.Errorf("invalid array length %d", length)
		}

		array := make([]interface{}, 0, length)
		for i := 0; i < length; i++ {
			value, err := readRESPValue(reader)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}

		return array, nil
	default:
		return nil, fmt.Errorf("unsupported RESP type byte %q", prefix)
	}
}

func readRESPLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2:] != "\r\n" {
		return "", fmt.Errorf("RESP line missing CRLF terminator")
	}

	return line[:len(line)-2], nil
}

func encodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

func encodeBulkString(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

func encodeSimpleError(s string) []byte {
	return []byte("-" + s + "\r\n")
}
