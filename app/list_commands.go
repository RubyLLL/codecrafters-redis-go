package main

import (
	"strconv"
	"time"
)

func (s *server) handleRpush(args []string) []byte {
	if len(args) < 2 {
		return encodeWrongNumberOfArguments("rpush")
	}

	key := args[0]
	newValues := args[1:]

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
	}
	if ok && entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	list = append(list, newValues...)
	entry.value = redisValue{typ: listValue, list: list}
	s.store[key] = entry
	s.cond.Broadcast()

	return encodeInteger(len(list))
}

func (s *server) handleLrange(args []string) []byte {
	if len(args) != 3 {
		return encodeWrongNumberOfArguments("lrange")
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return encodeSimpleError(errIntegerOutOfRange)
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return encodeSimpleError(errIntegerOutOfRange)
	}

	entry, ok := s.getLiveEntry(key)

	if !ok {
		return encodeArray([]string{})
	}
	if entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	start, end, ok = normalizeListRange(start, end, len(list))
	if !ok {
		return encodeArray([]string{})
	}

	return encodeArray(list[start : end+1])
}

func (s *server) handleLpush(args []string) []byte {
	if len(args) < 2 {
		return encodeWrongNumberOfArguments("lpush")
	}

	key := args[0]
	newValues := args[1:]

	// reverse array
	for i, j := 0, len(newValues)-1; i < j; i, j = i+1, j-1 {
		newValues[i], newValues[j] = newValues[j], newValues[i]
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
	}
	if ok && entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	list = append(newValues, list...)
	entry.value = redisValue{typ: listValue, list: list}
	s.store[key] = entry
	s.cond.Broadcast()

	return encodeInteger(len(list))
}

func (s *server) handleLlen(args []string) []byte {
	if len(args) != 1 {
		return encodeWrongNumberOfArguments("llen")
	}

	entry, ok := s.getLiveEntry(args[0])

	if !ok {
		return encodeInteger(0)
	}

	if entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	return encodeInteger(len(entry.value.list))
}

func (s *server) handleLpop(args []string) []byte {
	if len(args) < 1 || len(args) > 2 {
		return encodeWrongNumberOfArguments("lpop")
	}

	key := args[0]
	deleteCount := 1
	hasCount := len(args) == 2
	if len(args) == 2 {
		var err error
		deleteCount, err = strconv.Atoi(args[1])
		if err != nil || deleteCount < 0 {
			return encodeSimpleError(errPositiveOutOfRange)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if !ok {
		if hasCount {
			return encodeNullArray()
		}
		return encodeNullBulkString()
	}
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
		if hasCount {
			return encodeNullArray()
		}
		return encodeNullBulkString()
	}
	if ok && entry.value.typ != listValue {
		return encodeSimpleError(errWrongType)
	}

	list := entry.value.list
	if len(list) == 0 {
		delete(s.store, key)
		if hasCount {
			return encodeArray([]string{})
		}
		return encodeNullBulkString()
	}
	if deleteCount == 0 {
		return encodeArray([]string{})
	}

	if deleteCount > len(list) {
		deleteCount = len(list)
	}

	popped := list[:deleteCount]
	remaining := list[deleteCount:]
	if len(remaining) == 0 {
		delete(s.store, key)
	} else {
		entry.value = redisValue{typ: listValue, list: remaining}
		s.store[key] = entry
	}

	if !hasCount {
		return encodeBulkString(popped[0])
	}

	return encodeArray(popped)
}

func (s *server) handleBlpop(args []string) []byte {
	if len(args) < 2 {
		return encodeWrongNumberOfArguments("blpop")
	}

	keys := args[:len(args)-1]
	timeout, err := strconv.ParseFloat(args[len(args)-1], 64)
	if err != nil || timeout < 0 {
		return encodeSimpleError(errInvalidTimeout)
	}

	deadline := time.Time{}
	if timeout > 0 {
		deadline = s.now().Add(time.Duration(timeout * float64(time.Second)))

		timer := time.AfterFunc(time.Until(deadline), func() {
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		})

		defer timer.Stop()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		for _, key := range keys {
			value, found, wrongType := s.popListHeadLocked(key)
			if wrongType {
				return encodeSimpleError(errWrongType)
			}
			if found {
				return encodeArray([]string{key, value})
			}
		}

		if !deadline.IsZero() && s.now().After(deadline) {
			return encodeNullArray()
		}

		s.cond.Wait()
	}
}

func (s *server) popListHeadLocked(key string) (string, bool, bool) {
	entry, ok := s.store[key]
	if !ok {
		return "", false, false
	}
	if entry.isExpired(s.now()) {
		delete(s.store, key)
		return "", false, false
	}
	if entry.value.typ != listValue {
		return "", false, true
	}

	list := entry.value.list
	if len(list) == 0 {
		delete(s.store, key)
		return "", false, false
	}

	value := list[0]
	remaining := list[1:]
	if len(remaining) == 0 {
		delete(s.store, key)
	} else {
		entry.value = redisValue{typ: listValue, list: remaining}
		s.store[key] = entry
	}

	return value, true, false
}

func normalizeListRange(start int, end int, length int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}

	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}

	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start >= length || start > end {
		return 0, 0, false
	}

	return start, end, true
}
