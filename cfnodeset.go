package cftransport

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	defaultConcurrency = 200
	banDuration        = 1 * time.Hour
)

// CfNodeStatus tracks a node and its ban state.
type CfNodeStatus struct {
	Node      CfNode
	BanExpire time.Time
}

// isBanned returns true if the node is currently banned.
func (s *CfNodeStatus) isBanned() bool {
	return !s.BanExpire.IsZero() && time.Now().Before(s.BanExpire)
}

// CfNodeSet manages a set of Cloudflare edge nodes.
type CfNodeSet struct {
	mu   sync.Mutex
	list []CfNodeStatus
}

// NewCfNodeSet creates an empty node set.
func NewCfNodeSet() *CfNodeSet {
	return &CfNodeSet{}
}

// pingResult holds the result of a ping test for sorting.
type pingResult struct {
	node     CfNode
	delay    time.Duration
	lossRate float64
}

// Refresh discovers and ranks Cloudflare edge nodes.
// It generates candidate IPs, performs TCP ping to narrow down to 5*topN,
// then HTTP ping to rank and select the final topN nodes.
func (s *CfNodeSet) Refresh(topN int) error {
	candidates := generateCandidateIPs()
	if len(candidates) == 0 {
		return nil
	}

	// Phase 1: TCP ping all candidates concurrently
	tcpResults := concurrentTCPPing(candidates, 1*time.Second)

	// Sort by loss rate then delay
	sort.Slice(tcpResults, func(i, j int) bool {
		if tcpResults[i].lossRate != tcpResults[j].lossRate {
			return tcpResults[i].lossRate < tcpResults[j].lossRate
		}
		return tcpResults[i].delay < tcpResults[j].delay
	})

	// Take top 5N for HTTP ping phase
	httpCandidateCount := topN * 5
	if httpCandidateCount > len(tcpResults) {
		httpCandidateCount = len(tcpResults)
	}
	tcpTop := tcpResults[:httpCandidateCount]

	// Phase 2: HTTP ping the top candidates
	httpNodes := make([]CfNode, len(tcpTop))
	for i, r := range tcpTop {
		httpNodes[i] = r.node
	}
	httpResults := concurrentHTTPPing(httpNodes, 3*time.Second)

	// Sort by loss rate then delay
	sort.Slice(httpResults, func(i, j int) bool {
		if httpResults[i].lossRate != httpResults[j].lossRate {
			return httpResults[i].lossRate < httpResults[j].lossRate
		}
		return httpResults[i].delay < httpResults[j].delay
	})

	// Phase 3: Construct the final list, respecting existing bans
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect currently banned nodes
	bannedSet := make(map[CfNode]time.Time)
	for _, ns := range s.list {
		if ns.isBanned() {
			bannedSet[ns.Node] = ns.BanExpire
		}
	}

	// Build new list from HTTP results, skipping banned nodes
	var newList []CfNodeStatus
	for _, r := range httpResults {
		if len(newList) >= topN {
			break
		}
		if _, banned := bannedSet[r.node]; banned {
			continue
		}
		newList = append(newList, CfNodeStatus{Node: r.node})
	}

	// Preserve banned entries in the list
	for node, expire := range bannedSet {
		newList = append(newList, CfNodeStatus{Node: node, BanExpire: expire})
	}

	s.list = newList
	return nil
}

// FilterOptions configures the Filter method.
type FilterOptions struct {
	EliminateSpeed float64       // bytes/s — below this, ban the node
	TargetSpeed    float64       // bytes/s — above this, accept immediately
	EliminateDelay time.Duration // above this delay, ban the node
	TargetDelay    time.Duration // below this delay, accept immediately
	TopN           int
}

// Filter tests nodes and returns the best ones matching the criteria.
func (s *CfNodeSet) Filter(opts FilterOptions) []string {
	s.mu.Lock()
	// Make a shuffled copy of the list indices
	indices := make([]int, len(s.list))
	for i := range indices {
		indices[i] = i
	}
	rand.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})
	s.mu.Unlock()

	type testedNode struct {
		node  CfNode
		speed float64
	}

	var accepted []string
	var tested []testedNode

	for _, idx := range indices {
		if len(accepted) >= opts.TopN {
			break
		}

		s.mu.Lock()
		ns := &s.list[idx]
		if ns.isBanned() {
			s.mu.Unlock()
			continue
		}
		node := ns.Node
		s.mu.Unlock()

		// Test delay first (cheaper)
		delay, _, err := node.TCPPing(2 * time.Second)
		if err != nil || (opts.EliminateDelay > 0 && delay > opts.EliminateDelay) {
			s.mu.Lock()
			s.list[idx].BanExpire = time.Now().Add(banDuration)
			s.mu.Unlock()
			continue
		}
		if opts.TargetDelay > 0 && delay <= opts.TargetDelay {
			accepted = append(accepted, string(node))
			tested = append(tested, testedNode{node: node, speed: 0})
			continue
		}

		// Test speed
		speed, err := node.SpeedTest(5 * time.Second)
		if err != nil || (opts.EliminateSpeed > 0 && speed < opts.EliminateSpeed) {
			s.mu.Lock()
			s.list[idx].BanExpire = time.Now().Add(banDuration)
			s.mu.Unlock()
			continue
		}
		if opts.TargetSpeed > 0 && speed >= opts.TargetSpeed {
			accepted = append(accepted, string(node))
			tested = append(tested, testedNode{node: node, speed: speed})
			continue
		}

		// Neither eliminated nor target-worthy, keep as candidate
		tested = append(tested, testedNode{node: node, speed: speed})
	}

	if len(accepted) >= opts.TopN {
		return accepted[:opts.TopN]
	}

	// Not enough nodes met the target — apply fallback threshold
	if len(tested) == 0 {
		return accepted
	}

	// Find fastest speed
	var fastest float64
	for _, t := range tested {
		if t.speed > fastest {
			fastest = t.speed
		}
	}

	threshold := opts.TargetSpeed
	if fastest > threshold {
		threshold = fastest
	}
	threshold *= 0.618

	// Collect nodes above threshold
	var remaining []string
	for _, t := range tested {
		if t.speed >= threshold {
			// Avoid duplicates with already accepted
			alreadyIn := false
			for _, a := range accepted {
				if a == string(t.node) {
					alreadyIn = true
					break
				}
			}
			if !alreadyIn {
				remaining = append(remaining, string(t.node))
			}
		} else if threshold > 0 {
			// Ban nodes below threshold
			s.mu.Lock()
			for i := range s.list {
				if s.list[i].Node == t.node {
					s.list[i].BanExpire = time.Now().Add(banDuration)
					break
				}
			}
			s.mu.Unlock()
		}
	}

	// Merge accepted + remaining, return topN
	result := append(accepted, remaining...)
	if len(result) > opts.TopN {
		return result[:opts.TopN]
	}
	return result
}

// concurrentTCPPing runs TCP ping on all nodes concurrently.
func concurrentTCPPing(nodes []CfNode, timeout time.Duration) []pingResult {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []pingResult
		sem     = make(chan struct{}, defaultConcurrency)
	)

	for _, node := range nodes {
		wg.Add(1)
		sem <- struct{}{}
		go func(n CfNode) {
			defer wg.Done()
			defer func() { <-sem }()
			delay, lossRate, err := n.TCPPing(timeout)
			if err != nil {
				return
			}
			mu.Lock()
			results = append(results, pingResult{node: n, delay: delay, lossRate: lossRate})
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	return results
}

// concurrentHTTPPing runs HTTP ping on all nodes concurrently.
func concurrentHTTPPing(nodes []CfNode, timeout time.Duration) []pingResult {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []pingResult
		sem     = make(chan struct{}, defaultConcurrency)
	)

	for _, node := range nodes {
		wg.Add(1)
		sem <- struct{}{}
		go func(n CfNode) {
			defer wg.Done()
			defer func() { <-sem }()
			delay, lossRate, err := n.HTTPPing(timeout)
			if err != nil {
				return
			}
			mu.Lock()
			results = append(results, pingResult{node: n, delay: delay, lossRate: lossRate})
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	return results
}

// Ban marks a node as banned for the default ban duration.
func (s *CfNodeSet) Ban(node CfNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.list {
		if s.list[i].Node == node {
			s.list[i].BanExpire = time.Now().Add(banDuration)
			return
		}
	}
}

// List returns a copy of the current node status list.
func (s *CfNodeSet) List() []CfNodeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]CfNodeStatus, len(s.list))
	copy(cp, s.list)
	return cp
}
