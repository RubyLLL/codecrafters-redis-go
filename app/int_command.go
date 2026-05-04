package main

func (s *server) handleIncr(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("incr")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := args[0]
	entry, ok := s.store[key]
	if !ok || entry.isExpired(s.now()) {
		v := redisValue{typ: intValue, number: 1}
		entry := storeEntry{value: v}
		s.store[key] = entry
		return encodeInteger(1)
	}

	if entry.value.typ != intValue {

	}
	current := entry.value.number
	entry.value = redisValue{typ: intValue, number: current + 1}
	s.store[key] = entry
	return encodeInteger(current + 1)
}
