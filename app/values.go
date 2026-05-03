package main

import "time"

type valueType int

const (
	stringValue valueType = iota
	listValue
	streamValue
)

type redisValue struct {
	typ    valueType
	str    string
	list   []string
	stream stream
}

type storeEntry struct {
	value     redisValue
	expiresAt time.Time
}

func (e storeEntry) isExpired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

type stream struct {
	entries []streamEntry
}

type streamEntry struct {
	id     streamID
	fields []streamField
}

type streamID struct {
	ms  int64
	seq int64
}

type streamField struct {
	key   string
	value string
}
