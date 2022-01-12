package impl

import (
	"fmt"
	"math/rand"
	"sync"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

/* ==== SafeRoutingTable ==== */

// Thread-safe wrapper for the routing table
type SafeRoutingTable struct {
	sync.Mutex
	table peer.RoutingTable
}

func (r *SafeRoutingTable) Set(key, val string) {
	r.Lock()
	defer r.Unlock()

	r.table[key] = val
}

func (r *SafeRoutingTable) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.table, key)
}

// Returns empty string if key doesn't exist in the routing table
func (r *SafeRoutingTable) Get(key string) string {
	r.Lock()
	defer r.Unlock()

	return r.table[key]
}

/* ========== AckChannels ========== */

// Thread-safe map which maps pktID -> channel
// Used for asynchronous notification
// When we process an AckMessage we send a signal to the corresponding packets channel
type AckChannels struct {
	sync.Mutex
	channelsMap map[string]chan bool
}

func (r *AckChannels) Set(key string, val chan bool) chan bool {
	r.Lock()
	defer r.Unlock()

	r.channelsMap[key] = val
	return val
}

func (r *AckChannels) Get(key string) (chan bool, bool) {
	r.Lock()
	defer r.Unlock()

	val, ok := r.channelsMap[key]
	return val, ok
}

func (r *AckChannels) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.channelsMap, key)
}

/* ========== SearchChannels ========== */

// Thread-safe map which maps requestID -> channel
// Used for asynchronous notification of full matched filenames
// When we process an SearchReplyMessage we send a filename (only if its fully matched) to the corresponding channel
type SearchChannels struct {
	sync.Mutex
	channelsMap map[string]chan string
}

func (r *SearchChannels) Set(key string, val chan string) chan string {
	r.Lock()
	defer r.Unlock()

	r.channelsMap[key] = val
	return val
}

func (r *SearchChannels) Get(key string) (chan string, bool) {
	r.Lock()
	defer r.Unlock()

	val, ok := r.channelsMap[key]
	return val, ok
}

func (r *SearchChannels) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.channelsMap, key)
}

/* ========== DataChannels ========== */

// Thread-safe map which maps requestID -> channel
// Used for asynchronous notification
// When we process an DataReplyMessage we send a pointer to it to the corresponding channel
type DataChannels struct {
	sync.Mutex
	channelsMap map[string]chan *types.DataReplyMessage
}

func (r *DataChannels) Set(key string, val chan *types.DataReplyMessage) chan *types.DataReplyMessage {
	r.Lock()
	defer r.Unlock()

	r.channelsMap[key] = val
	return val
}

func (r *DataChannels) Get(key string) (chan *types.DataReplyMessage, bool) {
	r.Lock()
	defer r.Unlock()

	val, ok := r.channelsMap[key]
	return val, ok
}

func (r *DataChannels) Delete(key string) {
	r.Lock()
	defer r.Unlock()

	delete(r.channelsMap, key)
}

/* ======== Set ========= */

// Simple set implementation - not thread-safe
type Set struct {
	set map[string]struct{}
}

func (s *Set) Get() map[string]struct{} {
	return s.set
}

func NewSet(elems ...string) *Set {
	s := &Set{set: make(map[string]struct{})}
	for _, elem := range elems {
		s.Add(elem)
	}
	return s
}

func (s *Set) Add(elem string) *Set {
	s.set[elem] = struct{}{}
	return s
}

func (s *Set) Delete(elem string) {
	delete(s.set, elem)
}

func (s *Set) Contains(elem string) bool {
	_, ok := s.set[elem]
	return ok
}

func (s *Set) Len() int {
	return len(s.set)
}

/* ======== Peers ======== */

// Stores all known peers:
// thread-safe wrapper over Set
type ThreadSafeSet struct {
	sync.Mutex
	peersSet Set
}

func (p *ThreadSafeSet) Add(peer string) {
	p.Lock()
	defer p.Unlock()

	p.peersSet.Add(peer)
}

func (p *ThreadSafeSet) Delete(peer string) {
	p.Lock()
	defer p.Unlock()

	p.peersSet.Delete(peer)
}

func (p *ThreadSafeSet) Contains(peer string) bool {
	p.Lock()
	defer p.Unlock()

	return p.peersSet.Contains(peer)
}

func (p *ThreadSafeSet) Len() int {
	p.Lock()
	defer p.Unlock()
	return p.peersSet.Len()
}

// Returns random peer (empty string if there are none)
// ignorePeers: peers to be ignored
func (p *ThreadSafeSet) GetRandomPeer(ignorePeers *Set) string {
	peers := p.GetAllPeers(ignorePeers)

	if len(peers) == 0 {
		return ""
	}

	return peers[rand.Intn(len(peers))]
}

func (p *ThreadSafeSet) GetAllPeers(ignorePeers *Set) []string {
	p.Lock()
	defer p.Unlock()

	peers := make([]string, 0)

	for key := range p.peersSet.set {
		if !ignorePeers.Contains(key) {
			peers = append(peers, key)
		}
	}

	return peers
}

// Return n random peers, if len(known peers) < n ~> returns all known peers
func (p *ThreadSafeSet) GetNRandomPeers(n int, ignorePeers *Set) []string {
	peers := p.GetAllPeers(ignorePeers)
	if len(peers) < n {
		return peers
	}

	nPeers := make([]string, 0)
	len := len(peers)

	for i := 0; i < n; i++ {
		randIndex := rand.Intn(len)
		nPeers = append(nPeers, peers[randIndex])
		peers[randIndex], peers[len-1] = peers[len-1], peers[randIndex]
		len--
	}

	return nPeers
}

/* ======== RumorHistory ======== */
type RumorHistory struct {
	sync.Mutex
	rumorMap map[string][]types.Rumor
}

func (r *RumorHistory) AddRumor(rumor types.Rumor) bool {
	r.Lock()
	defer r.Unlock()

	expected := uint(len(r.rumorMap[rumor.Origin])) + 1
	if rumor.Sequence != expected {
		return false
	}

	r.rumorMap[rumor.Origin] = append(r.rumorMap[rumor.Origin], rumor)
	return true
}

func (r *RumorHistory) GetRumor(origin string, sequence uint) (types.Rumor, error) {
	r.Lock()
	defer r.Unlock()

	rumors := r.rumorMap[origin]
	if uint(len(rumors)) > sequence {
		return types.Rumor{}, fmt.Errorf("no rumor with sequence %d found for peer %s",
			sequence, origin)
	}
	return rumors[sequence-1], nil
}

// Returns all received rumors from origin with sequence numbers >= startSeq
func (r *RumorHistory) GetRumorsRange(origin string, startSeq uint) ([]types.Rumor, error) {
	r.Lock()
	defer r.Unlock()

	if startSeq < 1 {
		return make([]types.Rumor, 0), fmt.Errorf("invalid startSeq: %d", startSeq)
	}

	rumors := r.rumorMap[origin]

	if uint(len(rumors)) < startSeq {
		return make([]types.Rumor, 0), fmt.Errorf("no rumors with start sequence %d found for peer %s",
			startSeq, origin)
	}
	return rumors[startSeq-1:], nil
}

func (r *RumorHistory) GetLastSeq(origin string) uint {
	r.Lock()
	defer r.Unlock()

	rumors := r.rumorMap[origin]
	if len(rumors) == 0 {
		return 0
	}
	return rumors[len(rumors)-1].Sequence
}

// Creates rumor from given msg and adds it to history before returning it
func (r *RumorHistory) AddNewRumor(msg transport.Message, origin string) types.Rumor {
	r.Lock()
	defer r.Unlock()

	expected := uint(len(r.rumorMap[origin])) + 1
	rumor := types.Rumor{
		Origin:   origin,
		Sequence: expected,
		Msg:      &msg}

	r.rumorMap[origin] = append(r.rumorMap[origin], rumor)

	return rumor
}

// Returns map: origin -> sequence number of the last rumor received by this origin
func (r *RumorHistory) GetView() map[string]uint {
	r.Lock()
	defer r.Unlock()
	view := make(map[string]uint)

	for origin, rumors := range r.rumorMap {
		view[origin] = rumors[len(rumors)-1].Sequence
	}

	return view
	// FIXME: maybe keep view in struct instead of dynamically computing it
}

/* ======== PeersCatalog ======== */
type PeersCatalog struct {
	sync.Mutex
	catalog peer.Catalog
}

func (p *PeersCatalog) GetRandomPeer(key string) string {
	p.Lock()
	defer p.Unlock()
	peers := p.catalog[key]

	if len(peers) == 0 {
		return ""
	}

	randomIndex := rand.Intn(len(peers))
	i := 0

	for k := range peers {
		if i == randomIndex {
			return k
		}
		i++
	}
	return ""
}

/* ========= Proposer ============= */
type Proposer struct {
	sync.Mutex
	currID                        uint
	promiseThresholdReached       chan bool
	acceptThresholdReached        chan types.PaxosValue
	isListeningToAcceptThreshold  bool
	isListeningToPromiseThreshold bool
	paxosPhase                    uint
	recvPromises                  uint
	highestAcceptID               uint
	highestAcceptVal              *types.PaxosValue
	acceptedVals                  map[string]*Set // Value UniqID -> set of acceptors
	tlcBroadcasted                bool
}

func (p *Proposer) ResetHighestAcceptVal() {
	p.Lock()
	p.highestAcceptID = 0
	p.highestAcceptVal = nil
	p.Unlock()
}

func (p *Proposer) SetListeningToAcceptThreshold(b bool) {
	p.Lock()
	defer p.Unlock()

	p.isListeningToAcceptThreshold = b
}

func (p *Proposer) SetListeningToPromiseThreshold(b bool) {
	p.Lock()
	defer p.Unlock()

	p.isListeningToPromiseThreshold = b
}

func (p *Proposer) SetPhase(phase uint) {
	p.Lock()
	p.paxosPhase = phase
	p.Unlock()
}

func (p *Proposer) GetPhase() uint {
	p.Lock()
	defer p.Unlock()
	return p.paxosPhase
}

func (p *Proposer) ResetRecvPromises() {
	p.Lock()
	defer p.Unlock()
	p.recvPromises = 0
}

func (p *Proposer) SetCurrID(ID uint) {
	p.Lock()
	p.currID = ID
	p.Unlock()
}

func (p *Proposer) GetCurrID() uint {
	p.Lock()
	defer p.Unlock()
	return p.currID
}

func (p *Proposer) SetTlcBroadcasted(b bool) {
	p.Lock()
	p.tlcBroadcasted = b
	p.Unlock()
}

func (p *Proposer) IsTlcBroadcasted() bool {
	p.Lock()
	defer p.Unlock()
	return p.tlcBroadcasted
}

/* ========= TlcCounter ============= */
type TlcCounter struct {
	sync.Mutex
	counter map[uint][]*types.TLCMessage
}

func (c *TlcCounter) Append(step uint, msg *types.TLCMessage) {
	c.Lock()
	c.counter[step] = append(c.counter[step], msg)
	c.Unlock()
}

func (c *TlcCounter) Len(step uint) int {
	c.Lock()
	defer c.Unlock()
	return len(c.counter[step])
}

// returns representative element and length of list
func (c *TlcCounter) Get(step uint) (*types.TLCMessage, int) {
	c.Lock()
	defer c.Unlock()
	len := len(c.counter[step])
	if len > 0 {
		return c.counter[step][0], len
	}

	return nil, len
}

func (c *TlcCounter) Delete(step uint) {
	c.Lock()
	defer c.Unlock()
	delete(c.counter, step)
}

/* ========= PageRankTlcCounter ============= */
type PageRankTlcCounter struct {
	sync.Mutex
	counter map[uint][]*types.PageRankTLCMessage
}

func (c *PageRankTlcCounter) Append(step uint, msg *types.PageRankTLCMessage) {
	c.Lock()
	c.counter[step] = append(c.counter[step], msg)
	c.Unlock()
}

func (c *PageRankTlcCounter) Len(step uint) int {
	c.Lock()
	defer c.Unlock()
	return len(c.counter[step])
}

// returns representative element and length of list
func (c *PageRankTlcCounter) Get(step uint) (*types.PageRankTLCMessage, int) {
	c.Lock()
	defer c.Unlock()
	len := len(c.counter[step])
	if len > 0 {
		return c.counter[step][0], len
	}

	return nil, len
}

func (c *PageRankTlcCounter) Delete(step uint) {
	c.Lock()
	defer c.Unlock()
	delete(c.counter, step)
}

/* ========= AcceptedProposal ============= */
type AcceptedProposal struct {
	sync.Mutex
	acceptedID    uint
	acceptedValue *types.PaxosValue
}

func (p *AcceptedProposal) GetAccepted() (uint, *types.PaxosValue) {
	p.Lock()
	defer p.Unlock()

	return p.acceptedID, p.acceptedValue
}

func (p *AcceptedProposal) SetAccepted(acceptedID uint, acceptedValue *types.PaxosValue) {
	p.Lock()
	defer p.Unlock()
	p.acceptedID = acceptedID
	p.acceptedValue = acceptedValue
}

/* ========= SafeUint ============= */
type SafeUint struct {
	sync.Mutex
	val uint
}

func (p *SafeUint) Set(val uint) {
	p.Lock()
	defer p.Unlock()
	p.val = val
}

func (p *SafeUint) Get() uint {
	p.Lock()
	defer p.Unlock()
	return p.val
}

func (p *SafeUint) Increment() {
	p.Lock()
	defer p.Unlock()
	p.val++
}

/* ========= SafeBool ============= */
type SafeBool struct {
	sync.Mutex
	val bool
}

func (p *SafeBool) Set(val bool) {
	p.Lock()
	defer p.Unlock()
	p.val = val
}

func (p *SafeBool) Get() bool {
	p.Lock()
	defer p.Unlock()
	return p.val
}

/*  ========= SearchEngineResponseStorage ============= */

type SearchEngineResponseStorage struct {
	sync.Mutex
	responses map[string]map[string]struct{}
}

func (s *SearchEngineResponseStorage) AddResponses(requestID string, res []string) {
	s.Lock()
	defer s.Unlock()

	for _, website := range res {
		s.responses[requestID][website] = struct{}{}
	}
}

func (s *SearchEngineResponseStorage) GetResponses(requestID string) map[string]struct{} {
	s.Lock()
	defer s.Unlock()
	_, ok := s.responses[requestID]

	if ok {
		return s.responses[requestID]
	} else {
		return nil
	}
}
