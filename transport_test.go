package cftransport

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testURL = "https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js"

// TestTransport_RealRoundRobin uses Refresh-discovered CF IPs to verify
// round-robin routing through cdn.jsdelivr.net (CF-fronted CDN).
func TestTransport_RealRoundRobin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	// Refresh to discover real CF nodes
	s := NewCfNodeSet()
	if err := s.Refresh(2); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	list := s.List()
	var pool []poolEntry
	for _, ns := range list {
		if !ns.IsBanned() {
			pool = append(pool, poolEntry{ip: string(ns.Node)})
		}
	}
	if len(pool) == 0 {
		t.Fatal("no active nodes after Refresh")
	}
	t.Logf("using %d nodes from Refresh: %v", len(pool), pool)

	tr := &Transport{
		nodeSet: s,
		opts:    FilterOptions{TopN: 2, EliminateDelay: 10 * time.Second},
		pool:    pool,
	}

	client := &http.Client{Transport: tr, Timeout: 15 * time.Second}

	var successes int
	for i := 0; i < 4; i++ {
		resp, err := client.Get(testURL)
		if err != nil {
			t.Logf("request %d failed: %v", i, err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			successes++
		} else {
			t.Logf("request %d: status=%d", i, resp.StatusCode)
		}
	}

	if successes == 0 {
		t.Fatal("all requests failed — no CF node could serve jsdelivr")
	}
	t.Logf("round-robin: %d/4 succeeded via discovered CF nodes", successes)
}

func TestTransport_FailoverAndBan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	s := NewCfNodeSet()
	s.mu.Lock()
	s.list = []CfNodeStatus{
		{Node: CfNode("192.0.2.1")}, // TEST-NET, unreachable
	}
	s.mu.Unlock()

	tr := &Transport{
		nodeSet: s,
		opts:    FilterOptions{TopN: 1, EliminateDelay: 1 * time.Second},
		pool: []poolEntry{
			{ip: "192.0.2.1"},
		},
	}

	client := &http.Client{Transport: tr, Timeout: 2 * time.Second}

	for i := 0; i < 3; i++ {
		_, err := client.Get(testURL)
		if err == nil {
			t.Fatal("expected connection error to unreachable IP")
		}
	}

	if tr.PoolSize() != 0 {
		t.Errorf("expected empty pool after 3 failures, got %d", tr.PoolSize())
	}
	list := s.List()
	for _, ns := range list {
		if ns.Node == "192.0.2.1" && !ns.IsBanned() {
			t.Error("expected 192.0.2.1 to be banned after consecutive failures")
		}
	}
}

func TestTransport_FailoverToGoodNode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	// Refresh to get a known-good node
	s := NewCfNodeSet()
	if err := s.Refresh(1); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	list := s.List()
	var goodIP string
	for _, ns := range list {
		if !ns.IsBanned() {
			goodIP = string(ns.Node)
			break
		}
	}
	if goodIP == "" {
		t.Fatal("no active node found after Refresh")
	}
	t.Logf("good node: %s", goodIP)

	// Add an unreachable node to the pool alongside the good one
	s.mu.Lock()
	s.list = append(s.list, CfNodeStatus{Node: CfNode("192.0.2.1")})
	s.mu.Unlock()

	tr := &Transport{
		nodeSet: s,
		opts:    FilterOptions{TopN: 2, EliminateDelay: 1 * time.Second},
		pool: []poolEntry{
			{ip: "192.0.2.1"}, // bad first
			{ip: goodIP},      // good second
		},
	}

	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	var successes, failures int
	for i := 0; i < 6; i++ {
		resp, err := client.Get(testURL)
		if err != nil {
			failures++
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			successes++
		}
	}

	t.Logf("successes=%d, failures=%d, pool_size=%d", successes, failures, tr.PoolSize())
	if successes == 0 {
		t.Error("expected at least some successful requests through the good node")
	}
}

func TestTransport_SuccessResetsFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	// Refresh to get a known-good node
	s := NewCfNodeSet()
	if err := s.Refresh(1); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	list := s.List()
	var goodIP string
	for _, ns := range list {
		if !ns.IsBanned() {
			goodIP = string(ns.Node)
			break
		}
	}
	if goodIP == "" {
		t.Fatal("no active node found after Refresh")
	}

	tr := &Transport{
		nodeSet: s,
		opts:    FilterOptions{TopN: 1, EliminateDelay: 10 * time.Second},
		pool: []poolEntry{
			{ip: goodIP},
		},
	}

	client := &http.Client{Transport: tr, Timeout: 15 * time.Second}
	resp, err := client.Get(testURL)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	tr.mu.RLock()
	if len(tr.pool) > 0 && tr.pool[0].failures != 0 {
		t.Errorf("expected failures=0 after success, got %d", tr.pool[0].failures)
	}
	tr.mu.RUnlock()

	if tr.PoolSize() != 1 {
		t.Errorf("expected pool size 1, got %d", tr.PoolSize())
	}
	t.Logf("success via %s, failures reset correctly", goodIP)
}

// TestTransport_LocalRoundRobin is a fast unit test using a local httptest server.
func TestTransport_LocalRoundRobin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	s := NewCfNodeSet()
	s.mu.Lock()
	s.list = []CfNodeStatus{{Node: CfNode("127.0.0.1")}}
	s.mu.Unlock()

	tr := &Transport{
		nodeSet: s,
		opts:    FilterOptions{TopN: 2, EliminateDelay: 5 * time.Second},
		pool: []poolEntry{
			{ip: "127.0.0.1"},
			{ip: "127.0.0.1"},
		},
	}

	client := &http.Client{Transport: tr}
	for i := 0; i < 4; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
	}

	if tr.index.Load() != 4 {
		t.Errorf("expected index=4, got %d", tr.index.Load())
	}
}

// TestTransport_LocalBanOnFailure uses a closed port to verify ban logic.
func TestTransport_LocalBanOnFailure(t *testing.T) {
	closedListener, _ := net.Listen("tcp", "127.0.0.1:0")
	closedPort := closedListener.Addr().(*net.TCPAddr).Port
	closedListener.Close()

	s := NewCfNodeSet()
	s.mu.Lock()
	s.list = []CfNodeStatus{{Node: CfNode("127.0.0.1")}}
	s.mu.Unlock()

	tr := &Transport{
		nodeSet: s,
		opts:    FilterOptions{TopN: 1, EliminateDelay: 2 * time.Second},
		pool:    []poolEntry{{ip: "127.0.0.1"}},
	}

	client := &http.Client{Transport: tr, Timeout: 1 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d/", closedPort)

	for i := 0; i < 3; i++ {
		client.Get(url)
	}

	if tr.PoolSize() != 0 {
		t.Errorf("expected empty pool after 3 failures, got %d", tr.PoolSize())
	}
}
