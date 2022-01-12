package types

// SearchEngineRequestMessage describes a request to search for a website.
//
// - implements types.Message
type SearchEngineRequestMessage struct {
	// RequestID must be a unique identifier. Use xid.New().String() to generate
	// it.
	RequestID string
	// Origin is the address of the peer that initiated the search request.
	Origin string

	// Specify if the regexp is also done is the content of the website
	ContentSearch bool

	// use regexp.MustCompile(Pattern) to convert a string to a regexp.Regexp
	Pattern string
	Budget  uint
}

// SearchReplyMessage describes the response of a search request.
//
// - implements types.Message
// - implemented in HW2
type SearchEngineReplyMessage struct {
	// RequestID must be the same as the RequestID set in the
	// SearchRequestMessage.
	RequestID string

	Responses []string //FIXME maybe to change based on how website are implemented
}
