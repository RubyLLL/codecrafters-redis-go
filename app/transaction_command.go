package main

func (c *clientSession) handleMulti(args []string) []byte {
	if len(args) != 0 {
		return encodeWrongNumberOfArguments("multi")
	}
	if c.transactional {
		return encodeSimpleError(errNestedMulti)
	}

	c.transactional = true
	c.queue = nil

	return encodeSimpleString(OK)
}

func (c *clientSession) handleExec(args []string) []byte {
	if len(args) != 0 {
		c.transactional = false
		c.queue = nil
		return encodeSimpleError(errExecAbort + " wrong number of arguments for 'exec' command")
	}
	if !c.transactional {
		return encodeSimpleError(errExecWithoutMulti)
	}

	queued := c.queue
	c.queue = nil
	c.transactional = false

	result := make([][]byte, 0, len(queued))
	for _, command := range queued {
		result = append(result, c.server.executeCommand(command))
	}

	return encodeRawArray(result)
}
