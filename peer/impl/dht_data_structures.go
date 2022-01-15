package impl

import (
	"container/list"
	"sync"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
)

/* ========== ContactsChannels ========== */

// Thread-safe map which maps FindNode requestID -> channel
// Used for asynchronous notification
// When we process an FindNodeReply we send a pointer to it to the corresponding channel
type ContactsChannels struct {
	sync.Mutex
	channelsMap map[string]chan *types.FindNodeReply
}

func (r *ContactsChannels) Set(key string, val chan *types.FindNodeReply) chan *types.FindNodeReply {
	r.Lock()
	defer r.Unlock()

	r.channelsMap[key] = val
	return val
}

func (r *ContactsChannels) Get(key string) (chan *types.FindNodeReply, bool) {
	r.Lock()
	defer r.Unlock()

	val, ok := r.channelsMap[key]
	return val, ok
}

func (r *ContactsChannels) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.channelsMap, key)
}

/* ========== ValueChannels ========== */

// Thread-safe map which maps FindValue requestID -> channel
// Used for asynchronous notification
// When we process an FindValueReply we send a pointer to it to the corresponding channel
type ValueChannels struct {
	sync.Mutex
	channelsMap map[string]chan *types.FindValueReply
}

func (r *ValueChannels) Set(key string, val chan *types.FindValueReply) chan *types.FindValueReply {
	r.Lock()
	defer r.Unlock()

	r.channelsMap[key] = val
	return val
}

func (r *ValueChannels) Get(key string) (chan *types.FindValueReply, bool) {
	r.Lock()
	defer r.Unlock()

	val, ok := r.channelsMap[key]
	return val, ok
}

func (r *ValueChannels) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.channelsMap, key)
}

/* ========== KBucket ========== */
type KBucket struct {
	sync.Mutex
	K        int
	Contacts list.List
}

// Returns list element of contact in list with equal id to given contact.
// Returns nil if element not found
func (bucket *KBucket) GetElement(contact types.Contact) *list.Element {
	bucket.Lock()
	defer bucket.Unlock()

	for elem := bucket.Contacts.Front(); elem != nil; elem = elem.Next() {
		elem_val, _ := elem.Value.(types.Contact)
		if EqualContacts(&elem_val, &contact) {
			return elem
		}
	}
	return nil
}

// Adds given contact to bucket if alegible by least-recently seen eviction policy
// decribed in the Kademlia paper.
func (bucket *KBucket) AddContact(contact types.Contact) bool {
	elem := bucket.GetElement(contact)
	bucket.Lock()
	defer bucket.Unlock()

	if elem != nil {
		bucket.Contacts.MoveToFront(elem)
		return true
	} else {
		if bucket.Contacts.Len() < peer.K {
			bucket.Contacts.PushFront(contact)
			return true
		}
		// TODO: bucket full
		// ping head
		// if responds {
		// 		MoveToFront(head)
		// } else {
		//		remove head
		//		insert contact at tail
		//	}
		return false
	}
}

/* ========== KademliaRoutingTable ========== */
type KademliaRoutingTable struct {
	KBuckets []KBucket
}

/* ========== SafeByteMap ========== */

// Thread-safe map which maps requestID -> channel
// Used for asynchronous notification of full matched filenames
// When we process an SearchReplyMessage we send a filename (only if its fully matched) to the corresponding channel
type SafeByteMap struct {
	sync.Mutex
	byteMap map[string][]byte
}

func (r *SafeByteMap) Set(key string, val []byte) []byte {
	r.Lock()
	defer r.Unlock()

	r.byteMap[key] = val
	return val
}

func (r *SafeByteMap) Append(key, addr string) []byte {
	r.Lock()
	defer r.Unlock()

	addrList, ok := r.byteMap[key]
	if !ok {
		r.byteMap[key] = []byte(addr)
	} else {
		addrList := append(addrList, []byte(peer.MetafileSep)...)
		addrList = append(addrList, []byte(addr)...)
		r.byteMap[key] = addrList
	}

	return r.byteMap[key]
}

func (r *SafeByteMap) Get(key string) ([]byte, bool) {
	r.Lock()
	defer r.Unlock()

	val, ok := r.byteMap[key]
	return val, ok
}

func (r *SafeByteMap) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.byteMap, key)
}
