package impl

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"go.dedis.ch/cs438/peer"
)

// func (n *node) EditPointerRecord(privateKey *rsa.PrivateKey, record peer.PointerRecord,
// 	newValue string, newTtl uint) (peer.PointerRecord, error) {
// 	return n.CreatePointerRecord(privateKey, newValue, record.Sequence+1, newTtl)
// }

func (n *node) CreatePointerRecord(privateKey *rsa.PrivateKey, value string, sequence, ttl uint) (peer.PointerRecord, error) {
	var record peer.PointerRecord
	record.Value = value
	record.Sequence = sequence
	record.TTL = ttl
	record.PublicKey = &privateKey.PublicKey

	byteRecord, err := json.Marshal(record)
	if err != nil {
		return record, err
	}

	rng := rand.Reader
	hashed := sha256.Sum256(byteRecord)

	signature, err := rsa.SignPKCS1v15(rng, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return record, err
	}

	record.Signature = signature

	err = n.ValidatePointerRecord(record, record.PublicKey)

	return record, err
}

func (n *node) ValidatePointerRecord(record peer.PointerRecord, publicKey *rsa.PublicKey) error {
	signature := record.Signature
	record.Signature = nil

	byteRecord, err := json.Marshal(record)
	if err != nil {
		return err
	}

	hashed := sha256.Sum256(byteRecord)

	err = rsa.VerifyPKCS1v15(record.PublicKey, crypto.SHA256, hashed[:], signature)
	if err != nil {
		return err
	}
	return nil
}

func (n *node) PublishPointerRecord(record peer.PointerRecord) (string, error) {
	byteRecord, err := json.Marshal(record)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(x509.MarshalPKCS1PublicKey(record.PublicKey))
	hashHex := hex.EncodeToString(hash[:])

	n.Store(hashHex, byteRecord)
	return hashHex, nil
}

func (n *node) FetchPointerRecord(hash string) (peer.PointerRecord, bool) {
	var record peer.PointerRecord
	if byteRecord, ok := n.FindValue(hash); ok {
		err := json.Unmarshal(byteRecord, &record)
		if err != nil {
			return record, false
		}

		// validate that record address is the hash of the record's public key
		hashPubKey := sha256.Sum256(x509.MarshalPKCS1PublicKey(record.PublicKey))
		hashHex := hex.EncodeToString(hashPubKey[:])
		if hashHex != hash {
			return record, false
		}

		err = n.ValidatePointerRecord(record, record.PublicKey)
		if err != nil {
			return record, false
		}
		return record, true
	}
	return record, false
}
