package main

func (s *server) handleMulti(args []string) []byte {
	s.transactional = true

	return encodeSimpleString(OK)
}
