package cftransport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TransportOptions configures the Transport.
type TransportOptions struct {
	UpstreamCount  int           // number of IPs to keep in rotation
	EliminateDelay time.Duration // dial timeout; connections slower than this fail
}

// upstream holds a cached http.Transport for a specific IP.
type upstream struct {
	ip        string
	transport *http.Transport
}

// Transport implements http.RoundTripper with Cloudflare IP load balancing.
type Transport struct {
	manager *PoolManager
	opts    TransportOptions
	index   atomic.Uint64

	mu        sync.Mutex
	upstreams atomic.Pointer[[]upstream]
}

// Transport creates an http.RoundTripper backed by this PoolManager.
func (m *PoolManager) Transport(opts TransportOptions) *Transport {
	t := &Transport{
		manager: m,
		opts:    opts,
	}
	t.refreshUpstreams()
	return t
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	up := t.nextUpstream()
	if up == nil {
		return nil, fmt.Errorf("cftransport: no available upstream nodes")
	}

	resp, err := up.transport.RoundTrip(req)
	if err != nil {
		t.manager.RecordFailure(CfNode(up.ip))
		t.maybeRefillUpstreams(up.ip)
		return nil, err
	}

	t.manager.RecordSuccess(CfNode(up.ip))
	return resp, nil
}

func (t *Transport) nextUpstream() *upstream {
	ptr := t.upstreams.Load()
	if ptr == nil {
		return nil
	}
	ups := *ptr
	if len(ups) == 0 {
		return nil
	}
	idx := int(t.index.Add(1)-1) % len(ups)
	return &ups[idx]
}

// maybeRefillUpstreams checks if the failed IP was removed,
// and if so, refreshes the upstream list.
func (t *Transport) maybeRefillUpstreams(failedIP string) {
	ptr := t.upstreams.Load()
	if ptr == nil {
		t.refreshUpstreams()
		return
	}
	ups := *ptr
	for _, u := range ups {
		if u.ip == failedIP {
			return // still there (hasn't hit max failures yet)
		}
	}
	t.refreshUpstreams()
}

func (t *Transport) refreshUpstreams() {
	t.mu.Lock()
	defer t.mu.Unlock()

	ips := t.manager.Take(t.opts.UpstreamCount, &t.index)
	if len(ips) == 0 {
		t.manager.RefreshPool()
		ips = t.manager.Take(t.opts.UpstreamCount, &t.index)
	}

	// Close old transports
	if old := t.upstreams.Load(); old != nil {
		for _, u := range *old {
			u.transport.CloseIdleConnections()
		}
	}

	// Create new cached transports
	ups := make([]upstream, len(ips))
	for i, ip := range ips {
		ups[i] = upstream{
			ip:        ip,
			transport: t.makeTransport(ip),
		}
	}
	t.upstreams.Store(&ups)
}

func (t *Transport) makeTransport(ip string) *http.Transport {
	timeout := t.opts.EliminateDelay
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
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
			return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, addr)
		},
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}
}

// PoolSize returns the current number of IPs in the underlying pool.
func (t *Transport) PoolSize() int {
	return t.manager.PoolLen()
}

// UpstreamSize returns how many IPs are currently in rotation.
func (t *Transport) UpstreamSize() int {
	ptr := t.upstreams.Load()
	if ptr == nil {
		return 0
	}
	return len(*ptr)
}
