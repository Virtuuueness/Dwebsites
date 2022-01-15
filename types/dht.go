package types

import (
	"fmt"
)

// -----------------------------------------------------------------------------
// FindNodeReply

// NewEmpty implements types.Message.
func (d FindNodeReply) NewEmpty() Message {
	return &FindNodeReply{}
}

// Name implements types.Message.
func (d FindNodeReply) Name() string {
	return "findnodereply"
}

// String implements types.Message.
func (d FindNodeReply) String() string {
	return fmt.Sprintf("findnodereply")
}

// HTML implements types.Message.
func (d FindNodeReply) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
// FindNodeRequest

// NewEmpty implements types.Message.
func (d FindNodeRequest) NewEmpty() Message {
	return &FindNodeReply{}
}

// Name implements types.Message.
func (d FindNodeRequest) Name() string {
	return "findnoderequest"
}

// String implements types.Message.
func (d FindNodeRequest) String() string {
	return fmt.Sprintf("findnoderequest")
}

// HTML implements types.Message.
func (d FindNodeRequest) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
//  StoreRequest

// NewEmpty implements types.Message.
func (d StoreRequest) NewEmpty() Message {
	return &StoreRequest{}
}

// Name implements types.Message.
func (d StoreRequest) Name() string {
	return "storerequest"
}

// String implements types.Message.
func (d StoreRequest) String() string {
	return fmt.Sprintf("storerequest")
}

// HTML implements types.Message.
func (d StoreRequest) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
//  AppendRequest

// NewEmpty implements types.Message.
func (d AppendRequest) NewEmpty() Message {
	return &AppendRequest{}
}

// Name implements types.Message.
func (d AppendRequest) Name() string {
	return "appendrequest"
}

// String implements types.Message.
func (d AppendRequest) String() string {
	return fmt.Sprintf("appendrequest")
}

// HTML implements types.Message.
func (d AppendRequest) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
//  FindValueRequest

// NewEmpty implements types.Message.
func (d FindValueRequest) NewEmpty() Message {
	return &FindValueRequest{}
}

// Name implements types.Message.
func (d FindValueRequest) Name() string {
	return "findvaluerequest"
}

// String implements types.Message.
func (d FindValueRequest) String() string {
	return fmt.Sprintf("findvaluerequest")
}

// HTML implements types.Message.
func (d FindValueRequest) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
//  FindValueReply

// NewEmpty implements types.Message.
func (d FindValueReply) NewEmpty() Message {
	return &FindValueReply{}
}

// Name implements types.Message.
func (d FindValueReply) Name() string {
	return "findvaluereply"
}

// String implements types.Message.
func (d FindValueReply) String() string {
	return fmt.Sprintf("findvaluereply")
}

// HTML implements types.Message.
func (d FindValueReply) HTML() string {
	return d.String()
}
