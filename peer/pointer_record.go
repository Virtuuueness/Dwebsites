package peer

import "crypto/rsa"

type PointerRecord struct {
	Value     string //hash-pointer to immutable object
	Sequence  uint
	TTL       uint
	PublicKey *rsa.PublicKey
	Signature []byte
	Links     []string
}

type MutableRecord interface {
	CreateFolderPointerRecord(privateKey *rsa.PrivateKey, links []string, sequence, ttl uint) (PointerRecord, error)

	CreatePointerRecord(privateKey *rsa.PrivateKey, value string, sequence, ttl uint) (PointerRecord, error)

	ValidatePointerRecord(record PointerRecord, publicKey *rsa.PublicKey) error

	// Publishes given pointer record and returns it's identifier (hash of public key)
	PublishPointerRecord(record PointerRecord) (string, error)

	// Fetches pointer record (from local store or from peers) associated with given hash and returns it.
	// Returns error if record not found.
	FetchPointerRecord(hash string) (PointerRecord, bool)

	IsFolderRecord(record PointerRecord) bool
}
