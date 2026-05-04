package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const maxStreamSeq = int64(^uint64(0) >> 1)

func (s *server) handleXadd(args []string) []byte {
	if len(args) < 4 || len(args)%2 != 0 {
		return encodeWrongNumberOfArguments("xadd")
	}

	key := args[0]
	rawID := args[1]

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
	lastID, hasLastID := lastStreamID(entries)
	id, errMsg := parseStreamID(rawID, lastID, hasLastID)
	if errMsg != "" {
		return encodeSimpleError(errMsg)
	}
	if hasLastID && !isStreamIDAfter(id, lastID) {
		return encodeSimpleError(errStreamID)
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
	s.streamCond.Broadcast()

	return encodeBulkString(formatStreamID(id))
}

func (s *server) handleXrange(args []string) []byte {
	if len(args) != 3 {
		return encodeWrongNumberOfArguments("xrange")
	}

	key := args[0]
	start, ok := parseStreamIDForXrange(args[1], false)
	if !ok {
		return encodeSimpleError(errStreamIDType)
	}
	end, ok := parseStreamIDForXrange(args[2], true)
	if !ok {
		return encodeSimpleError(errStreamIDType)
	}

	entry, ok := s.getLiveEntry(key)
	if !ok {
		return encodeRawArray(nil)
	}
	if entry.value.typ != streamValue {
		return encodeSimpleError(errWrongType)
	}

	entries := entry.value.stream.entries
	result := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		if compareStreamID(entry.id, start) >= 0 && compareStreamID(entry.id, end) <= 0 {
			result = append(result, buildXrangeEntry(entry))
		}
	}

	return encodeRawArray(result)
}

func (s *server) handleXread(args []string) []byte {
	blocking := false
	timeout := time.Duration(0)

	if len(args) >= 1 && strings.EqualFold(args[0], "BLOCK") {
		if len(args) < 5 {
			return encodeWrongNumberOfArguments("xread")
		}
		ms, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || ms < 0 {
			return encodeSimpleError(errInvalidStreamTimeOut)
		}
		blocking = true
		timeout = time.Duration(ms) * time.Millisecond
		args = args[2:]
	}

	keys, ids, errResp := parseXreadStreams(args)
	if errResp != nil {
		return errResp
	}

	if blocking {
		return s.handleBlockingXread(keys, ids, timeout)
	}

	return s.handleUnblockingXread(keys, ids)
}

func parseXreadStreams(args []string) ([]string, []string, []byte) {
	if len(args) < 3 || !strings.EqualFold(args[0], "STREAMS") {
		return nil, nil, encodeWrongNumberOfArguments("xread")
	}
	if (len(args)-1)%2 != 0 {
		return nil, nil, encodeSimpleError(errUnbalancedXread)
	}

	pair := (len(args) - 1) / 2
	keys := args[1 : 1+pair]
	ids := args[1+pair:]

	return keys, ids, nil
}

func (s *server) handleBlockingXread(keys []string, ids []string, timeout time.Duration) []byte {
	deadline := time.Time{}
	if timeout > 0 {
		deadline = s.now().Add(timeout)

		timer := time.AfterFunc(time.Until(deadline), func() {
			s.mu.Lock()
			s.streamCond.Broadcast()
			s.mu.Unlock()
		})

		defer timer.Stop()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	starts, errResp := s.resolveXreadStartsLocked(keys, ids)
	if errResp != nil {
		return errResp
	}

	for {
		result, errResp := s.collectXreadResultsLocked(keys, starts)
		if errResp != nil {
			return errResp
		}
		if len(result) > 0 {
			return encodeRawArray(result)
		}

		if !deadline.IsZero() && !s.now().Before(deadline) {
			return encodeNullArray()
		}

		s.streamCond.Wait()
	}
}

func (s *server) handleUnblockingXread(keys []string, ids []string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	starts, errResp := s.resolveXreadStartsLocked(keys, ids)
	if errResp != nil {
		return errResp
	}

	result, errResp := s.collectXreadResultsLocked(keys, starts)
	if errResp != nil {
		return errResp
	}
	if len(result) == 0 {
		return encodeNullArray()
	}

	return encodeRawArray(result)
}

func (s *server) resolveXreadStartsLocked(keys []string, ids []string) ([]streamID, []byte) {
	starts := make([]streamID, 0, len(ids))
	for i, id := range ids {
		if id == "$" {
			lastID, errResp := s.lastStreamIDLocked(keys[i])
			if errResp != nil {
				return nil, errResp
			}
			starts = append(starts, lastID)
			continue
		}

		start, ok := parseStreamIDForXread(id)
		if !ok {
			return nil, encodeSimpleError(errStreamIDType)
		}
		starts = append(starts, start)
	}

	return starts, nil
}

func (s *server) lastStreamIDLocked(key string) (streamID, []byte) {
	entry, ok := s.store[key]
	if !ok {
		return streamID{}, nil
	}
	if entry.isExpired(s.now()) {
		delete(s.store, key)
		return streamID{}, nil
	}
	if entry.value.typ != streamValue {
		return streamID{}, encodeSimpleError(errWrongType)
	}

	lastID, ok := lastStreamID(entry.value.stream.entries)
	if !ok {
		return streamID{}, nil
	}

	return lastID, nil
}

func (s *server) collectXreadResultsLocked(keys []string, starts []streamID) ([][]byte, []byte) {
	result := make([][]byte, 0, len(keys))
	for i, key := range keys {
		entry, ok := s.store[key]
		if !ok {
			continue
		}
		if entry.isExpired(s.now()) {
			delete(s.store, key)
			continue
		}
		if entry.value.typ != streamValue {
			return nil, encodeSimpleError(errWrongType)
		}

		entries := entriesAfterID(entry.value.stream.entries, starts[i])
		if len(entries) == 0 {
			continue
		}

		result = append(result, encodeRawArray([][]byte{
			encodeBulkString(key),
			buildXreadEntries(entries),
		}))
	}

	return result, nil
}

func buildXrangeEntry(entry streamEntry) []byte {
	return encodeRawArray([][]byte{
		encodeBulkString(formatStreamID(entry.id)),
		buildArrayFromEntry(entry),
	})
}

func buildXreadEntries(entries []streamEntry) []byte {
	result := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		result = append(result, buildXrangeEntry(entry))
	}
	return encodeRawArray(result)
}

func buildArrayFromEntry(entry streamEntry) []byte {
	result := make([]string, 0, len(entry.fields)*2)
	for _, field := range entry.fields {
		result = append(result, field.key)
		result = append(result, field.value)
	}

	return encodeArray(result)
}

func parseStreamIDForXrange(id string, isEnd bool) (streamID, bool) {
	switch id {
	case "-":
		return streamID{}, true
	case "+":
		return streamID{ms: maxStreamSeq, seq: maxStreamSeq}, true
	}

	parts := strings.Split(id, "-")
	if len(parts) > 2 || len(parts) == 0 {
		return streamID{}, false
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return streamID{}, false
	}

	if len(parts) == 1 {
		seq := int64(0)
		if isEnd {
			seq = maxStreamSeq
		}
		return streamID{ms: ms, seq: seq}, true
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return streamID{}, false
	}

	return streamID{ms: ms, seq: seq}, true
}

func parseStreamIDForXread(id string) (streamID, bool) {
	return parseStreamIDForXrange(id, false)
}

func entriesAfterID(entries []streamEntry, id streamID) []streamEntry {
	for i, entry := range entries {
		if compareStreamID(entry.id, id) > 0 {
			return entries[i:]
		}
	}
	return nil
}

func parseStreamID(id string, lastID streamID, hasLastID bool) (streamID, string) {
	if id == "0-0" {
		return streamID{}, errStreamZeroID
	}
	if strings.EqualFold(id, "*") {
		ms := time.Now().UnixMilli()
		return streamID{ms: ms, seq: nextStreamSequence(ms, lastID, hasLastID)}, ""
	}

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return streamID{}, errStreamIDType
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return streamID{}, errStreamIDType
	}

	if parts[1] == "*" {
		return streamID{ms: ms, seq: nextStreamSequence(ms, lastID, hasLastID)}, ""
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return streamID{}, errStreamIDType
	}

	return streamID{ms: ms, seq: seq}, ""
}

func lastStreamID(entries []streamEntry) (streamID, bool) {
	if len(entries) == 0 {
		return streamID{}, false
	}

	return entries[len(entries)-1].id, true
}

func nextStreamSequence(ms int64, lastID streamID, hasLastID bool) int64 {
	if ms == 0 {
		return 1
	}
	if hasLastID && ms == lastID.ms {
		return lastID.seq + 1
	}

	return 0
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

func compareStreamID(a streamID, b streamID) int {
	if a.ms < b.ms {
		return -1
	}
	if a.ms > b.ms {
		return 1
	}
	if a.seq < b.seq {
		return -1
	}
	if a.seq > b.seq {
		return 1
	}

	return 0
}

func formatStreamID(id streamID) string {
	return fmt.Sprintf("%d-%d", id.ms, id.seq)
}
