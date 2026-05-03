package main

import (
	"fmt"
	"strconv"
	"strings"
)

func (s *server) handleXadd(args []string) []byte {
	if len(args) < 4 || len(args)%2 != 0 {
		return encodeWrongNumberOfArguments("xadd")
	}

	key := args[0]
	rawID := args[1]

	id, errMsg := parseStreamID(rawID)
	if errMsg != "" {
		return encodeSimpleError(errMsg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.store[key]
	if ok && entry.isExpired(s.now()) {
		entry = storeEntry{}
		ok = false
		delete(s.store, key)
	}
	if ok && entry.value.typ != streamValue {
		return encodeSimpleError(errWrongType)
	}

	entries := entry.value.stream.entries
	if len(entries) > 0 {
		if !isStreamIDAfter(id, entries[len(entries)-1].id) {
			return encodeSimpleError(errStreamID)
		}
	}

	fields := make([]streamField, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		fields = append(fields, streamField{key: args[i], value: args[i+1]})
	}

	entries = append(entries, streamEntry{id: id, fields: fields})
	entry.value = redisValue{
		typ: streamValue,
		stream: stream{
			entries: entries,
		},
	}
	s.store[key] = entry

	return encodeBulkString(formatStreamID(id))
}

func parseStreamID(id string) (streamID, string) {
	if strings.EqualFold(id, "0-0") {
		return streamID{}, errStreamID
	}

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return streamID{}, errStreamIDType
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return streamID{}, errStreamIDType
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return streamID{}, errStreamIDType
	}

	return streamID{ms: ms, seq: seq}, ""
}

func isStreamIDAfter(id streamID, lastID streamID) bool {
	if id.ms > lastID.ms {
		return true
	}
	if id.ms == lastID.ms && id.seq > lastID.seq {
		return true
	}

	return false
}

func formatStreamID(id streamID) string {
	return fmt.Sprintf("%d-%d", id.ms, id.seq)
}
