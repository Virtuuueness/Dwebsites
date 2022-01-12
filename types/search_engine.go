package types

import (
	"fmt"
)

// -----------------------------------------------------------------------------
// SearchEngineRequestMessage

// NewEmpty implements types.Message.
func (s SearchEngineRequestMessage) NewEmpty() Message {
	return &SearchEngineRequestMessage{}
}

// Name implements types.Message.
func (s SearchEngineRequestMessage) Name() string {
	return "searchenginerequest"
}

// String implements types.Message.
func (s SearchEngineRequestMessage) String() string {
	return fmt.Sprintf("searchenginerequest{%s %t %d}", s.Pattern, s.ContentSearch, s.Budget)
}

// HTML implements types.Message.
func (s SearchEngineRequestMessage) HTML() string {
	return s.String()
}

// -----------------------------------------------------------------------------
// SearchEngineReplyMessage

// NewEmpty implements types.Message.
func (s SearchEngineReplyMessage) NewEmpty() Message {
	return &SearchEngineReplyMessage{}
}

// Name implements types.Message.
func (s SearchEngineReplyMessage) Name() string {
	return "searchenginereply"
}

// String implements types.Message.
func (s SearchEngineReplyMessage) String() string {
	return fmt.Sprintf("searchenginereply{%v}", s.Responses)
}

// HTML implements types.Message.
func (s SearchEngineReplyMessage) HTML() string {
	return fmt.Sprintf("searchenginereply %s", s.RequestID)
}
