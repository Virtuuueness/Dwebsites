package impl

import (
	"crypto"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"io"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"strings"
	"time"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//returns xor distance between two ids
func distance(id1, id2 big.Int) *big.Int {
	return big.NewInt(0).Xor(&id1, &id2)
}

// returns true if given contacts have equal ids
func EqualContacts(a *types.Contact, b *types.Contact) bool {
	return (a.Id.Cmp(&b.Id) == 0)
}

func NewContact(addr string) types.Contact {
	id := *big.NewInt(0)
	hash := sha1.Sum([]byte(addr))
	id.SetBytes(hash[:])
	return types.Contact{Id: id, Addr: addr}
}

// returns all contacts from this k-bucket as a slice
// (needed because contacts are stored in a list)
func (bucket *KBucket) GetContacts() []types.Contact {
	bucket.Lock()
	defer bucket.Unlock()
	contacts := make([]types.Contact, 0, 20)
	for i := bucket.Contacts.Front(); i != nil; i = i.Next() {
		if elem, ok := i.Value.(types.Contact); ok {
			contacts = append(contacts, elem)
		}
	}

	return contacts
}

// Adds given contact to appropriate bucket if alegible by least-recently seen eviction policy
// decribed in the Kademlia paper.
func (n *node) AddContact(contact types.Contact) {
	if EqualContacts(&types.Contact{Id: n.id, Addr: n.conf.Socket.GetAddress()}, &contact) {
		return // don't add yourself
	}

	index := n.GetKBucketIndexForID(contact.Id)
	n.kademliaRouter.KBuckets[index].AddContact(contact)
}

// returns the id of the k-bucket which would containt given id
func (n *node) GetKBucketIndexForID(id big.Int) int64 {
	dist := distance(n.id, id)

	// hack to find floor(log2(distance))
	// bitStr := fmt.Sprintf("%b", dist)
	// floorLog := len(bitStr) - 1

	floorLog := math.Floor(math.Log2(float64(dist.Uint64())))
	return int64(floorLog)
}

// returns K nodes closest to given id from local routing table
func (n *node) FindKClosest(id big.Int) []types.Contact {
	kNearest := make([]types.Contact, 0, peer.K)

	bucketIndex := n.GetKBucketIndexForID(id)
	if bucketIndex < 0 {
		bucketIndex = 0
	}

	bucket := &n.kademliaRouter.KBuckets[bucketIndex]

	if bucket != nil {
		kNearest = append(kNearest, bucket.GetContacts()...)
	}

	// all nodes in bucket to the left have closer distance to n.id than nodes in the buckets to the right

	if len(kNearest) < peer.K {
		// take nodes from all buckets to the left
		for i := bucketIndex - 1; (i >= 0) && (len(kNearest) < peer.K); i-- {
			// println("BUCKET INDEX ", i)
			bucket := &n.kademliaRouter.KBuckets[i]
			if bucket != nil {
				kNearest = append(kNearest, bucket.GetContacts()...)
			}
		}
	}

	if len(kNearest) < peer.K {
		// take nodes from all buckets to the right
		for i := bucketIndex + 1; (i < 160) && (len(kNearest) < peer.K); i++ {
			bucket := &n.kademliaRouter.KBuckets[i]
			if bucket != nil {
				kNearest = append(kNearest, bucket.GetContacts()...)
			}
		}
	}

	// sort by distance to n.id
	sort.Slice(kNearest, func(i, j int) bool {
		return distance(n.id, kNearest[i].Id).
			Cmp(distance(n.id, kNearest[j].Id)) == -1
	})

	return kNearest[:min(len(kNearest), peer.K)]
}

// Waits for reply whose ID is associated with given FindNodeRequest.
// Returns nil if no reply received until timeout
func (n *node) WaitForFindValueReply(findValueReq types.FindValueRequest) *types.FindValueReply {
	channel := n.valueChannels.Set(findValueReq.RequestID, make(chan *types.FindValueReply, 1))
	defer close(channel)
	defer n.valueChannels.Delete(findValueReq.RequestID)

	timer := time.NewTimer(n.conf.AckTimeout)

	select {
	case reply := <-channel:
		timer.Stop()
		return reply
	case <-timer.C:
		timer.Stop()
	}
	return nil
}

// Returns k closest nodes from given key that given peer has in his local routing table.
// Returns nil if peer doesn't respond until timeout
func (n *node) FetchKClosest(key, dest string) ([]byte, []types.Contact, error) {

	findValReq := types.FindValueRequest{RequestID: xid.New().String(), Key: key}
	err := n.DirectSend(dest, findValReq)
	if err != nil {
		return nil, nil, err
	}

	reply := n.WaitForFindValueReply(findValReq)
	if reply == nil {
		return nil, nil, nil
	}

	return reply.Value, reply.Contacts, nil
}

func removeDuplicates(contacts []types.Contact) []types.Contact {
	seen := NewSet()
	newContacts := make([]types.Contact, 0, len(contacts))

	for _, contact := range contacts {
		if seen.Contains(contact.Addr) {
			continue
		}

		newContacts = append(newContacts, contact)
		seen.Add(contact.Addr)
	}

	return newContacts
}

// Returns at most N closest contacts which have not been queried yet.
// Expects contacts sorted in ascending order (by distance)
func pickNUnqueried(contacts []types.Contact, N int, queried *ThreadSafeSet) []types.Contact {
	toQuery := make([]types.Contact, 0, N)

	// find alpha closest contacts
	cnt := 0
	for i := 0; i < len(contacts); i++ {
		if queried.Contains(contacts[i].Addr) {
			continue
		}

		toQuery = append(toQuery, contacts[i])

		cnt++
		if cnt == N {
			break
		}
	}

	return toQuery
}

// Concurrently queries contacts in toQuery, updates queried set, collects all results and merges
// with current KNearest contacts into a new list of K nearest contacts.
// Returns {sorted list of k nearest contacts, true if new closest node found, false otherwise}
// valLookup - bool indicating whether to return after first non-nil value has been found, or to
// return only after all queries are finished (true for FIND_VALUE, false for FIND_NODE)
func (n *node) Query(key string, currentKNearest, toQuery []types.Contact,
	queried *ThreadSafeSet, valLookup bool, cache bool) ([]byte, []types.Contact, bool) {
	newKNearest := make([]types.Contact, len(currentKNearest), peer.K)
	copy(newKNearest, currentKNearest)
	closer := false

	id := new(big.Int)
	id.SetString(key, 16)

	// var shortest_dist int64
	// shortest_dist = -1
	// var closest_contact types.Contact

	remoteContactsChan := make(chan []types.Contact, len(toQuery))
	remoteValueChan := make(chan []byte, len(toQuery))
	cacheContactsChan := make(chan types.Contact, len(toQuery)) // channel for contacts which did not return value

	// query the picked alpha contacts
	for _, contact := range toQuery {

		go func(addr string) {
			val, kNearestRemote, err := n.FetchKClosest(key, addr)
			if err != nil {
				log.Error().Msgf("[Kademlia] FetchKClosest()>: <%s>", err.Error())
				remoteValueChan <- nil
				if valLookup && cache {
					// send only if val is nil
					cacheContactsChan <- NewContact(addr)
				}
				remoteContactsChan <- nil
				return
			}
			queried.Add(addr)

			if kNearestRemote == nil {
				remoteValueChan <- val
				if cache && valLookup && (val == nil) {
					cacheContactsChan <- NewContact(addr)
				}
				remoteContactsChan <- nil
				return
			}

			// sort by distance to id
			sort.Slice(kNearestRemote, func(i, j int) bool {
				return distance(*id, kNearestRemote[i].Id).
					Cmp(distance(*id, kNearestRemote[j].Id)) == -1
			})

			remoteValueChan <- val
			if cache && valLookup && (val == nil) {
				cacheContactsChan <- NewContact(addr)
			}
			remoteContactsChan <- kNearestRemote[:min(len(kNearestRemote), peer.K)]
		}(contact.Addr)
	}

	//TODO: seperate this into its own function

	valueFoundChan := make(chan []byte)

	// merge fetched kNearest lists with a copy of currentKNearest
	go func() {
		valueFound := false
		var closestCacheContact types.Contact
		closestDist := int64(-1) // TODO: turn to Uint64
		valToCache := make([]byte, 0)
		for i := 0; i < len(toQuery); i++ {
			remoteVal := <-remoteValueChan
			if valLookup {
				if (remoteVal != nil) && !valueFound {
					valueFound = true
					valToCache = remoteVal
					valueFoundChan <- remoteVal
					// calling function will terminate, but we finish the for loop of this
					// go-routine to prevent hanging query go-routines
				}
				if remoteVal == nil && cache {
					cacheContact := <-cacheContactsChan
					dst := distance(*id, cacheContact.Id).Int64()
					if dst < closestDist {
						closestCacheContact = cacheContact
						closestDist = dst
					}
				}
			}

			kNearestRemote := <-remoteContactsChan

			if (valueFound && valLookup) || kNearestRemote == nil || len(kNearestRemote) == 0 {
				continue
			}

			if distance(*id, kNearestRemote[0].Id).Cmp(distance(*id, newKNearest[0].Id)) == -1 {
				closer = true
			}

			newKNearest = append(newKNearest, kNearestRemote...)
		}
		if !valLookup || !valueFound {
			valueFoundChan <- nil
		}
		close(remoteContactsChan)
		close(remoteValueChan)
		close(cacheContactsChan)

		if cache && valLookup && valueFound && (closestDist != -1) {
			// cache val at closest contact which did not return value
			n.StoreAtContact(key, valToCache, closestCacheContact)
		}
	}()

	val := <-valueFoundChan // blocks until value found or all k-nearest nodes fetched
	if valLookup && (val != nil) {
		return val, nil, false
	}

	//newKNearest is now updated by go-routine above

	newKNearest = removeDuplicates(newKNearest)
	// sort by distance to id
	sort.Slice(newKNearest, func(i, j int) bool {
		return distance(*id, newKNearest[i].Id).
			Cmp(distance(*id, newKNearest[j].Id)) == -1
	})

	// take first k
	newKNearest = newKNearest[:min(len(newKNearest), peer.K)]

	return nil, newKNearest, closer
}

func IsAllQueried(contacts []types.Contact, queried *ThreadSafeSet) bool {
	for _, contact := range contacts {
		if !queried.Contains(contact.Addr) {
			return false
		}
	}
	return true
}

// returns k nodes closest to given key by iteratively sending FIND_NODE
func (n *node) FindNode(key string) []types.Contact {
	id := new(big.Int)
	id.SetString(key, 16)
	kNearest := n.FindKClosest(*id)

	queried := ThreadSafeSet{peersSet: *NewSet(n.conf.Socket.GetAddress())}

	for {
		toQuery := pickNUnqueried(kNearest, peer.Alpha, &queried)

		_, newKNearest, closer := n.Query(key, kNearest, toQuery, &queried, false, false)

		if closer {
			kNearest = newKNearest
			continue
		}

		toQuery = pickNUnqueried(newKNearest, peer.K, &queried)
		_, newNewKNearest, closer := n.Query(key, newKNearest, toQuery, &queried, false, false)

		if closer || !IsAllQueried(newNewKNearest, &queried) {
			kNearest = newNewKNearest
			continue
		}

		// all found k closest nodes are queried and they haven't returned anything closer
		return newNewKNearest
	}
}

func (n *node) StoreAtContact(key string, value []byte, contact types.Contact) {
	storeReq := types.StoreRequest{Key: key, Value: value}
	err := n.DirectSend(contact.Addr, storeReq)
	if err != nil {
		log.Error().Msgf("[Kademlia] Store()>: <%s>", err.Error())
	}
}

func (n *node) StoreAppend(key, addr string) {
	kNearest := n.FindNode(key)

	for _, contact := range kNearest {
		go func(key, addr string, contact types.Contact) {
			appendReq := types.AppendRequest{Key: key, Addr: addr}
			err := n.DirectSend(contact.Addr, appendReq)
			if err != nil {
				log.Error().Msgf("[Kademlia] Append()>: <%s>", err.Error())
			}
		}(key, addr, contact)
	}
}

func (n *node) Store(key string, value []byte) {
	kNearest := n.FindNode(key)

	for _, contact := range kNearest {
		go n.StoreAtContact(key, value, contact)
		// go func(contact types.Contact) {
		// 	storeReq := types.StoreRequest{Key: key, Value: value}
		// 	err := n.DirectSend(contact.Addr, storeReq)
		// 	if err != nil {
		// 		log.Error().Msgf("[Kademlia] Store()>: <%s>", err.Error())
		// 	}
		// }(contact)
	}
}

// TODO: FindValue(pointerRecord) should wait for all values and pick one with largest sequence
// after that is implemented, caching can be turned on. Without caching we can be certain
// with high probability that returned value will be last inserted PointerRecord, but with caching
// we are likely to fetch an older record

// Fetches value associated with given key, returns {fetched value, true} if found,
// {empty byte array, false} otherwise
// Values stored in DHT are a list of nodes known to have data associated with given key
// ie. (addr1|addr2|addr3|addr4) where | is a MetaFileSep constant
func (n *node) FindValue(key string) ([]byte, bool) {
	if val, ok := n.localHashMap.Get(key); ok {
		return val, true
	}

	id := new(big.Int)
	id.SetString(key, 16)
	kNearest := n.FindKClosest(*id)

	queried := ThreadSafeSet{peersSet: *NewSet(n.conf.Socket.GetAddress())}

	for {
		toQuery := pickNUnqueried(kNearest, peer.Alpha, &queried)

		val, newKNearest, closer := n.Query(key, kNearest, toQuery, &queried, true, false)

		if val != nil {
			return val, true
		}

		if closer {
			kNearest = newKNearest
			continue
		}

		toQuery = pickNUnqueried(newKNearest, peer.K, &queried)
		val, newNewKNearest, closer := n.Query(key, newKNearest, toQuery, &queried, true, false)

		if val != nil {
			return val, true
		}

		if closer || !IsAllQueried(newNewKNearest, &queried) {
			kNearest = newNewKNearest
			continue
		}

		// all found k closest nodes are queried and they haven't returned anything closer and no value found
		return nil, false
	}
}

func (n *node) Bootstrap(peerAddr string) {
	hash := sha1.Sum([]byte(peerAddr))
	id := *big.NewInt(0)
	id.SetBytes(hash[:])

	n.AddContact(types.Contact{Id: id, Addr: peerAddr})
	n.AddPeer(peerAddr)
	n.FindNode(n.conf.Socket.GetAddress()) // lookup own node
	// TODO: refresh k-buckets further than closest neighbor
}

func (n *node) GetKademliaRouter() KademliaRoutingTable {
	return n.kademliaRouter
}

func (n *node) UploadDHT(data io.Reader) (metahash string, err error) {
	chunk := make([]byte, n.conf.ChunkSize)
	var metafile []byte
	var metafileKey []byte
	h := crypto.SHA256.New()

	for {
		num_bytes, err := data.Read(chunk)
		if err == io.EOF {
			break
		}

		_, err = h.Write(chunk[:num_bytes])
		if err != nil {
			return "", err
		}

		chunkHash := h.Sum(nil)
		chunkKey := hex.EncodeToString(chunkHash)

		slice_copy := make([]byte, num_bytes)
		copy(slice_copy, chunk[:num_bytes])

		n.conf.Storage.GetDataBlobStore().Set(chunkKey, slice_copy)

		// store chunk info in dht
		n.AppendToDHTEntry(chunkKey, n.conf.Socket.GetAddress())

		metafileKey = append(metafileKey, chunkHash...)

		// append to metafile
		if len(metafile) == 0 {
			metafile = append(metafile, chunkKey...)
		} else {
			metafile = append(metafile, []byte(peer.MetafileSep)...)
			metafile = append(metafile, chunkKey...)
		}

		h.Reset()
	}

	_, err = h.Write(metafileKey)
	if err != nil {
		return "", err
	}
	metahash = hex.EncodeToString(h.Sum(nil))
	n.conf.Storage.GetDataBlobStore().Set(metahash, metafile)
	// store metahash info in dht
	n.AppendToDHTEntry(metahash, n.conf.Socket.GetAddress())

	return metahash, nil
}

func (n *node) AppendToDHTEntry(key, addr string) {
	n.StoreAppend(key, addr)
	// if addrList, ok := n.FindValue(key); ok {
	// 	addrList = append(addrList, []byte(peer.MetafileSep)...)
	// 	addrList = append(addrList, []byte(addr)...)
	// 	n.Store(key, addrList)
	// } else {
	// 	n.Store(key, []byte(addr))
	// }
}

func (n *node) DownloadDHT(metahash string, keep bool) ([]byte, error) {
	metafile, err := n.FetchResourceDHT(metahash, keep)
	if err != nil {
		return nil, err
	}

	chunkKeys := strings.Split(string(metafile), peer.MetafileSep)

	var file []byte

	for _, chunkKey := range chunkKeys {
		chunk, err := n.FetchResourceDHT(chunkKey, keep)
		if err != nil {
			return nil, err
		}

		file = append(file, chunk...)
	}

	return file, nil
}

// Fetches value associated with given key from a random peer known to have it.
// Uses back-off strategy if peer is unresponsive.
func (n *node) FetchFromPeersDHT(key string) ([]byte, error) {
	var dest string
	if addrListStr, ok := n.FindValue(key); ok {
		addrList := strings.Split(string(addrListStr), peer.MetafileSep)
		dest = addrList[rand.Intn(len(addrList))]
		// println(key, " has n nodes: ", len(addrList), " fetching from ", dest)
	} else {
		return nil, fmt.Errorf("[peer.FetchFromPeers] no known peers hold key %s", key)
	}

	// Create DataRequestMessage
	dataReqMsg := types.DataRequestMessage{RequestID: xid.New().String(), Key: key}

	// Send DataRequestMessage to random peer from Catalog
	err := n.DirectSend(dest, dataReqMsg)
	if err != nil {
		return nil, err
	}

	reply, err := n.WaitForReply(dataReqMsg, dest)
	if err != nil {
		return nil, err
	}

	if len(reply.Value) == 0 {
		return nil, fmt.Errorf("[peer.FetchFromPeers] remote peer returned empty value")
	}

	if reply.Key != key {
		return nil, fmt.Errorf(`[peer.FetchFromPeers] key mishmatch:
								received reply with key %s, expeceted key: %s`, reply.Key, key)
	}

	return reply.Value, nil
}

// Fetches resource identified by given key, either from local store or from peers.
// Updates local DataBlobStore in case the resource is fetched from peers.
func (n *node) FetchResourceDHT(key string, keep bool) ([]byte, error) {
	// check locally
	resource := n.conf.Storage.GetDataBlobStore().Get(key)

	if resource == nil { // fetch from peers
		fetchedResource, err := n.FetchFromPeersDHT(key)
		if err != nil {
			return nil, err
		}
		// save locally
		n.conf.Storage.GetDataBlobStore().Set(key, fetchedResource)
		resource = fetchedResource
		if keep {
			go func() {
				n.AppendToDHTEntry(key, n.conf.Socket.GetAddress())
			}()
		}
	}

	return resource, nil
}

// for benchmarking
func (n *node) GetReqCnt() uint64 {
	return n.ReqCnt
}

func (n *node) ResetReqCnt() {
	n.ReqCnt = 0
}
