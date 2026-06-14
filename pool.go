package cftransport

import (
	"sync/atomic"
)

const (
	maxConsecutiveFailures = 3
	refreshThreshold       = 0.618
)

// Pool is an immutable snapshot of active IPs with failure tracking.
// Never mutated in place — the PoolManager replaces the entire Pool atomically.
type Pool struct {
	entries  []string
	failures map[string]int
}

func newPool(ips []string) *Pool {
	entries := make([]string, len(ips))
	copy(entries, ips)
	return &Pool{
		entries:  entries,
		failures: make(map[string]int),
	}
}

func emptyPool() *Pool {
	return &Pool{
		entries:  nil,
		failures: make(map[string]int),
	}
}

// Len returns the number of entries.
func (p *Pool) Len() int {
	if p == nil {
		return 0
	}
	return len(p.entries)
}

// Entries returns the IP list (read-only, do not modify).
func (p *Pool) Entries() []string {
	if p == nil {
		return nil
	}
	return p.entries
}

// Failures returns the failure count for an IP.
func (p *Pool) Failures(ip string) int {
	if p == nil {
		return 0
	}
	return p.failures[ip]
}

// withFailure returns a new Pool with the failure count incremented for ip.
func (p *Pool) withFailure(ip string) *Pool {
	entries := make([]string, len(p.entries))
	copy(entries, p.entries)
	failures := make(map[string]int, len(p.failures)+1)
	for k, v := range p.failures {
		failures[k] = v
	}
	failures[ip]++
	return &Pool{entries: entries, failures: failures}
}

// withSuccessReset returns a new Pool with the failure count cleared for ip.
func (p *Pool) withSuccessReset(ip string) *Pool {
	entries := make([]string, len(p.entries))
	copy(entries, p.entries)
	failures := make(map[string]int, len(p.failures))
	for k, v := range p.failures {
		if k != ip {
			failures[k] = v
		}
	}
	return &Pool{entries: entries, failures: failures}
}

// without returns a new Pool with the given IP removed.
func (p *Pool) without(ip string) *Pool {
	entries := make([]string, 0, len(p.entries))
	for _, e := range p.entries {
		if e != ip {
			entries = append(entries, e)
		}
	}
	failures := make(map[string]int, len(p.failures))
	for k, v := range p.failures {
		if k != ip {
			failures[k] = v
		}
	}
	return &Pool{entries: entries, failures: failures}
}

// --- PoolManager pool operations (CAS-based, lock-free) ---

// getPool loads the current pool atomically.
func (m *PoolManager) getPool() *Pool {
	return m.pool.Load()
}

// casPool attempts to swap old → new atomically. Returns true on success.
// Bumps version on success to signal pool state change.
func (m *PoolManager) casPool(old, new *Pool) bool {
	if m.pool.CompareAndSwap(old, new) {
		m.version.Add(1)
		return true
	}
	return false
}

// setPool stores a new pool unconditionally and bumps version.
func (m *PoolManager) setPool(p *Pool) {
	m.pool.Store(p)
	m.version.Add(1)
}

// RecordFailure increments failure count via CAS.
// If threshold hit, removes from pool and persists.
func (m *PoolManager) RecordFailure(node CfNode) {
	ip := string(node)
	for {
		p := m.getPool()
		if p == nil {
			return
		}

		newCount := p.failures[ip] + 1
		var next *Pool
		if newCount >= maxConsecutiveFailures {
			next = p.without(ip)
		} else {
			next = p.withFailure(ip)
		}

		if m.casPool(p, next) {
			if newCount >= maxConsecutiveFailures {
				m.savePool(next)
			}
			return
		}
		// CAS failed — retry with fresh pool
	}
}

// RecordSuccess resets failure count via CAS.
func (m *PoolManager) RecordSuccess(node CfNode) {
	ip := string(node)
	for {
		p := m.getPool()
		if p == nil || p.failures[ip] == 0 {
			return
		}
		next := p.withSuccessReset(ip)
		if m.casPool(p, next) {
			return
		}
	}
}

// RefreshPool checks pool health and refreshes if below threshold.
// "Healthy" count excludes nodes that have recorded failures.
func (m *PoolManager) RefreshPool() {
	p := m.getPool()
	if p == nil {
		m.ForceRefreshPool()
		return
	}
	healthy := p.Len() - len(p.failures)
	if float64(healthy) > float64(m.poolSize)*refreshThreshold {
		return
	}
	m.ForceRefreshPool()
}

// ForceRefreshPool runs Refresh and replaces the pool.
// Concurrent calls are deduplicated: only the first does work.
func (m *PoolManager) ForceRefreshPool() {
	vBefore := m.version.Load()

	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	// If version changed while waiting, another goroutine already refreshed
	if m.version.Load() != vBefore {
		return
	}
	m.Refresh()
}

// Take returns up to n IPs from the pool via round-robin.
// n is capped at poolSize. If remaining < 2n, triggers background refresh.
func (m *PoolManager) Take(n int, index *atomic.Uint64) []string {
	if n > m.poolSize {
		n = m.poolSize
	}

	p := m.getPool()
	if p == nil || p.Len() == 0 {
		return nil
	}

	count := n
	if count > p.Len() {
		count = p.Len()
	}

	result := make([]string, count)
	for i := 0; i < count; i++ {
		idx := int(index.Add(1)-1) % p.Len()
		result[i] = p.entries[idx]
	}

	remaining := p.Len() - count
	if remaining < 2*n {
		go m.ForceRefreshPool()
	}

	return result
}

// PoolEntries returns the current pool IPs. Lock-free.
func (m *PoolManager) PoolEntries() []string {
	p := m.getPool()
	if p == nil {
		return nil
	}
	return p.entries
}

// PoolLen returns current pool size. Lock-free.
func (m *PoolManager) PoolLen() int {
	p := m.getPool()
	return p.Len()
}
