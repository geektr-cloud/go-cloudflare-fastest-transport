package cftransport

import (
	"testing"
	"time"
)

func TestGenerateCandidateIPs(t *testing.T) {
	nodes := generateCandidateIPs()
	if len(nodes) == 0 {
		t.Fatal("expected candidate IPs, got none")
	}
	t.Logf("generated %d candidate IPs", len(nodes))

	// Verify they look like valid IPv4 addresses
	for i, n := range nodes[:5] {
		if !isIPv4(string(n)) {
			t.Errorf("node %d (%s) is not IPv4", i, n)
		}
	}
}

func TestCfNodeSet_Refresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	s := NewCfNodeSet()
	err := s.Refresh(2) // small topN to keep test fast
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	list := s.List()
	if len(list) == 0 {
		t.Fatal("expected nodes after refresh, got none")
	}

	// At least some should not be banned
	var active int
	for _, ns := range list {
		if !ns.isBanned() {
			active++
			t.Logf("active node: %s", ns.Node)
		}
	}
	if active == 0 {
		t.Error("no active (unbanned) nodes found after refresh")
	}
}

func TestCfNodeSet_Refresh_PreservesBan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	s := NewCfNodeSet()
	// Pre-populate with a banned node
	s.mu.Lock()
	s.list = []CfNodeStatus{
		{Node: "192.0.2.99", BanExpire: time.Now().Add(1 * time.Hour)},
	}
	s.mu.Unlock()

	err := s.Refresh(2)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// The banned node should still be present and banned
	list := s.List()
	found := false
	for _, ns := range list {
		if ns.Node == "192.0.2.99" {
			found = true
			if !ns.isBanned() {
				t.Error("expected 192.0.2.99 to remain banned")
			}
		}
	}
	if !found {
		t.Error("banned node 192.0.2.99 was lost during Refresh")
	}
}

func TestCfNodeSet_Filter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	s := NewCfNodeSet()
	// First refresh to populate
	err := s.Refresh(3)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	result := s.Filter(FilterOptions{
		EliminateDelay: 2 * time.Second,
		TargetDelay:    500 * time.Millisecond,
		TopN:           2,
	})

	t.Logf("Filter returned %d nodes: %v", len(result), result)
	if len(result) == 0 {
		t.Error("Filter returned no nodes")
	}
}

func TestCfNodeSet_Filter_SkipsBanned(t *testing.T) {
	s := NewCfNodeSet()
	s.mu.Lock()
	s.list = []CfNodeStatus{
		{Node: "104.16.132.229", BanExpire: time.Now().Add(1 * time.Hour)},
		{Node: "104.16.133.229", BanExpire: time.Time{}}, // not banned
	}
	s.mu.Unlock()

	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	result := s.Filter(FilterOptions{
		TargetDelay: 1 * time.Second,
		TopN:        2,
	})

	for _, ip := range result {
		if ip == "104.16.132.229" {
			t.Error("banned node should not appear in Filter results")
		}
	}
	t.Logf("Filter results: %v", result)
}

func TestCfNodeStatus_IsBanned(t *testing.T) {
	// Not banned (zero time)
	ns := CfNodeStatus{Node: "1.1.1.1"}
	if ns.isBanned() {
		t.Error("zero BanExpire should not be banned")
	}

	// Banned (future)
	ns.BanExpire = time.Now().Add(1 * time.Hour)
	if !ns.isBanned() {
		t.Error("future BanExpire should be banned")
	}

	// Expired ban (past)
	ns.BanExpire = time.Now().Add(-1 * time.Hour)
	if ns.isBanned() {
		t.Error("past BanExpire should not be banned")
	}
}
