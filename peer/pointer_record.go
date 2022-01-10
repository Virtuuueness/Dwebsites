package peer

import "crypto/rsa"

type PointerRecord struct {
	Name      string // File or folder name (for easier folder reconstruction)
	Value     string //hash-pointer to immutable object
	Sequence  uint
	TTL       uint
	PublicKey *rsa.PublicKey
	Signature []byte
	Links     []string
}

type MutableRecord interface {
	// Create a PointerRecord for a folder
	CreateFolderPointerRecord(privateKey *rsa.PrivateKey, name string, links []string, sequence, ttl uint) (PointerRecord, error)

	CreatePointerRecord(privateKey *rsa.PrivateKey, name string, value string, sequence, ttl uint) (PointerRecord, error)

	ValidatePointerRecord(record PointerRecord, publicKey *rsa.PublicKey) error

	// Publishes given pointer record and returns it's identifier (hash of public key)
	PublishPointerRecord(record PointerRecord) (string, error)

	// Fetches pointer record (from local store or from peers) associated with given hash and returns it.
	// Returns error if record not found.
	FetchPointerRecord(hash string) (PointerRecord, bool)

	// Respond whether folder is a record or not
	IsFolderRecord(record PointerRecord) bool

	// Create PointerRecord from folder at path with name folderName
	CreateAndPublishFolderRecord(path string, folderName string, privateKey *rsa.PrivateKey, sequence, ttl uint) (string, error)

	// Reconstruct the folder pointed at record in basePath folder
	ReconstructFolderFromRecord(basePath string, record PointerRecord) (string, error)
}
