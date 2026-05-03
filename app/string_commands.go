package main

func (s *server) handlePing(args []string) []byte {
	if len(args) > 1 {
		return encodeWrongNumberOfArguments("ping")
	}
	if len(args) == 1 {
		return encodeBulkString(args[0])
	}

	return encodeSimpleString("PONG")
}

func (s *server) handleEcho(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("echo")
	}

	return encodeBulkString(args[0])
}

func (s *server) handleType(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("type")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeSimpleString("none")
	}

	switch entry.value.typ {
	case stringValue:
		return encodeSimpleString("string")
	case listValue:
		return encodeSimpleString("list")
	case streamValue:
		return encodeSimpleString("stream")
	default:
		return encodeSimpleString("none")
	}
}

func (s *server) handleGet(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("get")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeNullBulkString()
	}

	if entry.value.typ != stringValue {
		return encodeSimpleError(errWrongType)
	}

	return encodeBulkString(entry.value.str)
}

func (s *server) handleSet(args []string) []byte {
	if len(args) != 2 && len(args) != 4 {
		return encodeWrongNumberOfArguments("set")
	}

	v := redisValue{
		typ: stringValue,
		str: args[1],
	}
	entry := storeEntry{value: v}
	if len(args) == 4 {
		expiresAt, err := s.parseSetExpiry(args[2], args[3])
		if err != nil {
			return encodeSimpleError(err.Error())
		}
		entry.expiresAt = expiresAt
	}

	s.mu.Lock()
	s.store[args[0]] = entry
	s.mu.Unlock()

	return encodeSimpleString(OK)
}
