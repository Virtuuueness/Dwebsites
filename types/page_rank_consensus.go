package types

import "fmt"

// -----------------------------------------------------------------------------
// PageRankPaxosPrepareMessage

// NewEmpty implements types.Message.
func (p PageRankPaxosPrepareMessage) NewEmpty() Message {
	return &PageRankPaxosPrepareMessage{}
}

// Name implements types.Message.
func (p PageRankPaxosPrepareMessage) Name() string {
	return "pagerankpaxosprepare"
}

// String implements types.Message.
func (p PageRankPaxosPrepareMessage) String() string {
	return fmt.Sprintf("{pagerankpaxosprepare %d - %d}", p.Step, p.ID)
}

// HTML implements types.Message.
func (p PageRankPaxosPrepareMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PageRankPaxosPromiseMessage

// NewEmpty implements types.Message.
func (p PageRankPaxosPromiseMessage) NewEmpty() Message {
	return &PageRankPaxosPromiseMessage{}
}

// Name implements types.Message.
func (p PageRankPaxosPromiseMessage) Name() string {
	return "pagerankpaxospromise"
}

// String implements types.Message.
func (p PageRankPaxosPromiseMessage) String() string {
	return fmt.Sprintf("{pagerankpaxospromise %d - %d (%d: %v)}", p.Step, p.ID,
		p.AcceptedID, p.AcceptedValue)
}

// HTML implements types.Message.
func (p PageRankPaxosPromiseMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PageRankPaxosProposeMessage

// NewEmpty implements types.Message.
func (p PageRankPaxosProposeMessage) NewEmpty() Message {
	return &PageRankPaxosProposeMessage{}
}

// Name implements types.Message.
func (p PageRankPaxosProposeMessage) Name() string {
	return "pagerankpaxospropose"
}

// String implements types.Message.
func (p PageRankPaxosProposeMessage) String() string {
	return fmt.Sprintf("{pagerankpaxospropose %d - %d (%v)}", p.Step, p.ID, p.Value)
}

// HTML implements types.Message.
func (p PageRankPaxosProposeMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PageRankPaxosAcceptMessage

// NewEmpty implements types.Message.
func (p PageRankPaxosAcceptMessage) NewEmpty() Message {
	return &PageRankPaxosAcceptMessage{}
}

// Name implements types.Message.
func (p PageRankPaxosAcceptMessage) Name() string {
	return "pagerankpaxosaccept"
}

// String implements types.Message.
func (p PageRankPaxosAcceptMessage) String() string {
	return fmt.Sprintf("{pagerankpaxosaccept %d - %d (%v)}", p.Step, p.ID, p.Value)
}

// HTML implements types.Message.
func (p PageRankPaxosAcceptMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PageRankTLCMessage

// NewEmpty implements types.Message.
func (p PageRankTLCMessage) NewEmpty() Message {
	return &PageRankTLCMessage{}
}

// Name implements types.Message.
func (p PageRankTLCMessage) Name() string {
	return "PageRanktlc"
}

// String implements types.Message.
func (p PageRankTLCMessage) String() string {
	return fmt.Sprintf("{PageRanktlc %d - (%v)}", p.Step, p.Block)
}

// HTML implements types.Message.
func (p PageRankTLCMessage) HTML() string {
	return p.String()
}
