package cftransport

import (
	"os"
	"path/filepath"
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
		if !ns.IsBanned() {
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
			if !ns.IsBanned() {
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
	if ns.IsBanned() {
		t.Error("zero BanExpire should not be banned")
	}

	// Banned (future)
	ns.BanExpire = time.Now().Add(1 * time.Hour)
	if !ns.IsBanned() {
		t.Error("future BanExpire should be banned")
	}

	// Expired ban (past)
	ns.BanExpire = time.Now().Add(-1 * time.Hour)
	if ns.IsBanned() {
		t.Error("past BanExpire should not be banned")
	}
}

func TestCfNodeSet_CSVPersistence(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "nodes.csv")

	// Create a node set with file, add some data
	s := NewCfNodeSetWithFile(csvPath)
	s.mu.Lock()
	s.list = []CfNodeStatus{
		{Node: "104.16.1.1", BanExpire: time.Time{}},
		{Node: "192.0.2.1", BanExpire: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	s.save()
	s.mu.Unlock()

	// Verify file was created
	if _, err := os.Stat(csvPath); err != nil {
		t.Fatalf("CSV file not created: %v", err)
	}

	// Load into a new node set
	s2 := NewCfNodeSetWithFile(csvPath)
	list := s2.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 nodes from CSV, got %d", len(list))
	}

	// Verify data
	if list[0].Node != "104.16.1.1" {
		t.Errorf("expected first node 104.16.1.1, got %s", list[0].Node)
	}
	if list[0].IsBanned() {
		t.Error("first node should not be banned")
	}
	if list[1].Node != "192.0.2.1" {
		t.Errorf("expected second node 192.0.2.1, got %s", list[1].Node)
	}
	if !list[1].IsBanned() {
		t.Error("second node should be banned (expires 2099)")
	}
}

func TestCfNodeSet_CSVPersistence_Ban(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "nodes.csv")

	s := NewCfNodeSetWithFile(csvPath)
	s.mu.Lock()
	s.list = []CfNodeStatus{
		{Node: "104.16.1.1"},
		{Node: "104.16.2.2"},
	}
	s.save()
	s.mu.Unlock()

	// Ban a node — should persist
	s.Ban("104.16.1.1")

	// Reload
	s2 := NewCfNodeSetWithFile(csvPath)
	list := s2.List()
	for _, ns := range list {
		if ns.Node == "104.16.1.1" && !ns.IsBanned() {
			t.Error("ban should persist to CSV")
		}
	}
}
