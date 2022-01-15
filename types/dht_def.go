package types

import (
	"math/big"
)

// PING: types.EmptyMessage{}

// entry in a k-bucket
type Contact struct {
	Id   big.Int
	Addr string
}

type FindValueRequest struct {
	RequestID string
	Key       string
}

type FindValueReply struct {
	RequestID string
	Value     []byte
	Contacts  []Contact
}

type FindNodeRequest struct {
	RequestID string
	Id        big.Int
}

type FindNodeReply struct {
	RequestID string
	Contacts  []Contact
}

type StoreRequest struct {
	Key   string
	Value []byte
}

type AppendRequest struct {
	Key  string
	Addr string
}
