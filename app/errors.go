package main

import "fmt"

const (
	OK     = "OK"
	QUEUED = "QUEUED"

	errEmptyCommand         = "ERR empty command"
	errUnknownCommand       = "ERR unknown command"
	errWrongType            = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errIntegerOutOfRange    = "ERR value is not an integer or out of range"
	errPositiveOutOfRange   = "ERR value is out of range, must be positive"
	errInvalidExpireTime    = "ERR invalid expire time in 'set' command"
	errInvalidTimeout       = "ERR timeout is not a float or out of range"
	errSyntax               = "ERR syntax error"
	errStreamID             = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	errStreamIDType         = "ERR Invalid stream ID specified as stream command argument"
	errStreamZeroID         = "ERR The ID specified in XADD must be greater than 0-0"
	errUnbalancedXread      = "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+' or '$' must be specified"
	errInvalidStreamTimeOut = "ERR timeout is not an integer or out of range"
	errExecWithoutMulti     = "ERR EXEC without MULTI"
	errExecAbort            = "EXECABORT Transaction discarded because of:"
	errNestedMulti          = "ERR MULTI calls can not be nested"
	errDiscardWithoutMulti  = "ERR DISCARD without MULTI"
)

func encodeWrongNumberOfArguments(command string) []byte {
	return encodeSimpleError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
}
