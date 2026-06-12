package cftransport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	maxConsecutiveFailures = 3
)

// Transport implements http.RoundTripper with Cloudflare IP load balancing.
type Transport struct {
	nodeSet *CfNodeSet
	opts    FilterOptions

	mu    sync.RWMutex
	pool  []poolEntry
	index atomic.Uint64
}

type poolEntry struct {
	ip       string
	failures int
}

// NewTransport creates a Transport backed by the given CfNodeSet.
// It immediately calls Filter to populate the IP pool.
func (s *CfNodeSet) Transport(opts FilterOptions) *Transport {
	t := &Transport{
		nodeSet: s,
		opts:    opts,
	}
	t.refreshPool()
	return t
}

// RoundTrip implements http.RoundTripper.
// It routes the request through a Cloudflare edge node using round-robin.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ip, idx := t.nextIP()
	if ip == "" {
		return nil, fmt.Errorf("cftransport: no available nodes")
	}

	// Create a transport that dials through the selected IP
	transport := &http.Transport{
		DialContext: t.dialContext(ip),
	}
	defer transport.CloseIdleConnections()

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.recordFailure(idx, ip)
		return nil, err
	}

	// Check if response time indicates an eliminate-worthy delay
	// (connection succeeded but was too slow — tracked by consecutive failures)
	t.recordSuccess(idx)
	return resp, nil
}

func (t *Transport) dialContext(ip string) func(ctx context.Context, network, address string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		// Extract port from original address
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			port = "443"
		}
		var addr string
		if strings.Contains(ip, ":") {
			addr = fmt.Sprintf("[%s]:%s", ip, port)
		} else {
			addr = fmt.Sprintf("%s:%s", ip, port)
		}
		return (&net.Dialer{
			Timeout: t.opts.EliminateDelay,
		}).DialContext(ctx, network, addr)
	}
}

func (t *Transport) nextIP() (string, int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.pool) == 0 {
		return "", -1
	}
	idx := int(t.index.Add(1)-1) % len(t.pool)
	return t.pool[idx].ip, idx
}

func (t *Transport) recordFailure(idx int, ip string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if idx < 0 || idx >= len(t.pool) || t.pool[idx].ip != ip {
		return
	}

	t.pool[idx].failures++
	if t.pool[idx].failures >= maxConsecutiveFailures {
		// Ban the node and remove from pool
		t.nodeSet.Ban(CfNode(ip))
		t.pool = append(t.pool[:idx], t.pool[idx+1:]...)

		// If pool is empty, refresh
		if len(t.pool) == 0 {
			t.mu.Unlock()
			t.refreshPool()
			t.mu.Lock()
		}
	}
}

func (t *Transport) recordSuccess(idx int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if idx >= 0 && idx < len(t.pool) {
		t.pool[idx].failures = 0
	}
}

func (t *Transport) refreshPool() {
	ips := t.nodeSet.Filter(t.opts)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pool = make([]poolEntry, len(ips))
	for i, ip := range ips {
		t.pool[i] = poolEntry{ip: ip}
	}
}

// PoolSize returns the current number of nodes in the active pool.
func (t *Transport) PoolSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.pool)
}
