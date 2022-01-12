package impl

import (
	"math"
	"sync"
)

type ranking struct {
	sync.Mutex
	ranking map[string]float64
}

type pageRank struct {
	sync.Mutex
	nodes               map[string]struct{}
	incommingLinks      map[string][]string // Directed edge of the graph
	nbOfOutcommingLinks map[string]int      // Number of outcoming link per node
}

func (r *pageRank) AddLink(from, to string) {
	r.Lock()
	defer r.Unlock()

	if from == to {
		return
	}

	//Add nodes to set of nodes

	r.nodes[from] = struct{}{}
	r.nodes[to] = struct{}{}

	//Add from node to list of incomming node in "to" node
	_, ok := r.incommingLinks[to]

	linkAlreadyExist := r.linkAlreadyExist(from, to)

	if ok {
		if !linkAlreadyExist {
			r.incommingLinks[to] = append(r.incommingLinks[to], from)
		}
	} else {
		r.incommingLinks[to] = []string{from}
	}

	//Increment counter if link does not already exist

	if !linkAlreadyExist {
		_, ok := r.nbOfOutcommingLinks[from]
		if ok {
			r.nbOfOutcommingLinks[from] = r.nbOfOutcommingLinks[from] + 1
		} else {
			r.nbOfOutcommingLinks[from] = 1
		}
	}

}

func (r *pageRank) LinkAlreadyExist(from, to string) bool {
	r.Lock()
	defer r.Unlock()

	return r.linkAlreadyExist(from, to)
}

func (r *pageRank) linkAlreadyExist(from, to string) bool {
	_, ok := r.incommingLinks[to]

	if !ok {
		return false
	}

	linkAlreadyExist := false
	for _, node := range r.incommingLinks[to] {
		if node == from {
			linkAlreadyExist = true
			break
		}
	}
	return linkAlreadyExist
}

func (r *pageRank) Rank() map[string]float64 {
	r.Lock()
	defer r.Unlock()
	nbOfNodes := len(r.nodes)
	startingRankPerNodes := 1.0 / float64(nbOfNodes)
	dampingFactor := 0.85
	convergenceThreshold := 0.01

	rank := make(map[string]float64, nbOfNodes)
	oldRank := make(map[string]float64, nbOfNodes)
	for n := range r.nodes {
		rank[n] = float64(startingRankPerNodes)
		oldRank[n] = 1.0
	}

	convergence := math.Inf(1)
	for convergence > convergenceThreshold {
		convergence = 0.0

		for node := range r.nodes {

			nodeRank := 0.0

			_, ok := r.incommingLinks[node]
			if ok {
				for _, incommingLinkNode := range r.incommingLinks[node] {
					nodeRank += oldRank[incommingLinkNode] / float64(r.nbOfOutcommingLinks[incommingLinkNode])
				}
			}

			rank[node] = (1.0-dampingFactor)/float64(nbOfNodes) + dampingFactor*nodeRank

			convergence += math.Abs(rank[node] - oldRank[node])
		}

		for node, ranking := range rank {
			oldRank[node] = ranking
		}
	}

	return rank
}
