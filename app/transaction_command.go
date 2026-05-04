package main

func (s *server) handleMulti(args []string) []byte {
	if len(args) != 0 {
		return encodeWrongNumberOfArguments("multi")
	}

	s.transactional = true

	return encodeSimpleString(OK)
}

func (s *server) handleExec(args []string) []byte {
	if len(args) != 0 {
		s.transactional = false
		return encodeSimpleError(errExecAbort + " wrong number of arguments for 'exec' command")
	}
	if s.transactional != true {
		return encodeSimpleError(errExecWithoutMulti)
	}

	result := make([][]byte, 0, len(s.queue))
	for _, command := range s.queue {
		result = append(result, s.executeCommand(command))
	}
	s.transactional = false

	return encodeRawArray(result)
}
