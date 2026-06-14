package cftransport

import (
	"encoding/csv"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultConcurrency = 200
)

// PoolManager manages discovery and rotation of Cloudflare edge node IPs.
type PoolManager struct {
	filePath string
	poolSize int

	pool      atomic.Pointer[Pool]
	version   atomic.Uint64
	refreshMu sync.Mutex // serializes ForceRefreshPool
}

// NewPoolManager creates a pool manager with the given pool size.
func NewPoolManager(poolSize int) *PoolManager {
	m := &PoolManager{poolSize: poolSize}
	m.pool.Store(emptyPool())
	return m
}

// NewPoolManagerWithFile creates a pool manager that persists IPs to a CSV file.
// If the file exists, the pool is loaded from it.
func NewPoolManagerWithFile(poolSize int, path string) *PoolManager {
	m := &PoolManager{poolSize: poolSize, filePath: path}
	ips := m.loadCSV()
	if len(ips) > 0 {
		m.setPool(newPool(ips))
	} else {
		m.pool.Store(emptyPool())
	}
	return m
}

// pingResult holds the result of a ping test for sorting.
type pingResult struct {
	node     CfNode
	delay    time.Duration
	lossRate float64
}

// Refresh discovers Cloudflare edge nodes and replaces the pool.
func (m *PoolManager) Refresh() error {
	candidates := generateCandidateIPs()
	if len(candidates) == 0 {
		return nil
	}

	// Phase 1: TCP ping all candidates
	tcpResults := concurrentTCPPing(candidates, 1*time.Second)

	sort.Slice(tcpResults, func(i, j int) bool {
		if tcpResults[i].lossRate != tcpResults[j].lossRate {
			return tcpResults[i].lossRate < tcpResults[j].lossRate
		}
		return tcpResults[i].delay < tcpResults[j].delay
	})

	// Take top 5×poolSize for HTTP ping
	httpCount := m.poolSize * 5
	if httpCount > len(tcpResults) {
		httpCount = len(tcpResults)
	}

	httpNodes := make([]CfNode, httpCount)
	for i := 0; i < httpCount; i++ {
		httpNodes[i] = tcpResults[i].node
	}

	// Phase 2: HTTP ping
	httpResults := concurrentHTTPPing(httpNodes, 3*time.Second)

	sort.Slice(httpResults, func(i, j int) bool {
		if httpResults[i].lossRate != httpResults[j].lossRate {
			return httpResults[i].lossRate < httpResults[j].lossRate
		}
		return httpResults[i].delay < httpResults[j].delay
	})

	// Take top poolSize
	count := m.poolSize
	if count > len(httpResults) {
		count = len(httpResults)
	}

	ips := make([]string, count)
	for i := 0; i < count; i++ {
		ips[i] = string(httpResults[i].node)
	}

	// Shuffle to distribute load
	rand.Shuffle(len(ips), func(i, j int) {
		ips[i], ips[j] = ips[j], ips[i]
	})

	// Replace pool and persist
	p := newPool(ips)
	m.setPool(p)
	m.savePool(p)
	return nil
}

// List returns the current pool IPs (convenience alias for PoolEntries).
func (m *PoolManager) List() []CfNode {
	entries := m.PoolEntries()
	nodes := make([]CfNode, len(entries))
	for i, e := range entries {
		nodes[i] = CfNode(e)
	}
	return nodes
}

// savePool persists the pool entries to CSV.
func (m *PoolManager) savePool(p *Pool) {
	if m.filePath == "" || p == nil {
		return
	}
	tmp := m.filePath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	w := csv.NewWriter(f)
	w.Write([]string{"ip"})
	for _, ip := range p.entries {
		w.Write([]string{ip})
	}
	w.Flush()
	f.Close()
	os.Rename(tmp, m.filePath)
}

// loadCSV reads IPs from the CSV file.
func (m *PoolManager) loadCSV() []string {
	if m.filePath == "" {
		return nil
	}
	f, err := os.Open(m.filePath)
	if err != nil {
		return nil
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil
	}

	var ips []string
	for i, record := range records {
		if i == 0 && len(record) > 0 && record[0] == "ip" {
			continue
		}
		if len(record) < 1 || record[0] == "" {
			continue
		}
		ips = append(ips, record[0])
	}
	return ips
}

// --- Concurrent ping helpers ---

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
