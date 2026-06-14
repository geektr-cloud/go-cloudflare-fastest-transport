package cftransport

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestGenerateCandidateIPs(t *testing.T) {
	nodes := generateCandidateIPs()
	if len(nodes) == 0 {
		t.Fatal("expected candidate IPs, got none")
	}
	t.Logf("generated %d candidate IPs", len(nodes))
}

func TestPoolManager_Refresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	m := NewPoolManager(3)
	if err := m.Refresh(); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	if m.PoolLen() == 0 {
		t.Fatal("expected nodes after refresh, got none")
	}
	t.Logf("refresh found %d nodes", m.PoolLen())
}

func TestPool_Immutable(t *testing.T) {
	p := newPool([]string{"a", "b", "c"})

	// withFailure produces a new pool, doesn't mutate original
	p2 := p.withFailure("a")
	if p.Failures("a") != 0 {
		t.Error("original pool mutated by withFailure")
	}
	if p2.Failures("a") != 1 {
		t.Errorf("new pool should have failures=1, got %d", p2.Failures("a"))
	}

	// without produces a new pool
	p3 := p.without("b")
	if p.Len() != 3 {
		t.Error("original pool mutated by without")
	}
	if p3.Len() != 2 {
		t.Errorf("expected 2 entries after without, got %d", p3.Len())
	}

	// withSuccessReset
	p4 := p2.withSuccessReset("a")
	if p4.Failures("a") != 0 {
		t.Error("withSuccessReset didn't clear failures")
	}
	if p2.Failures("a") != 1 {
		t.Error("original pool mutated by withSuccessReset")
	}
}

func TestPool_Without(t *testing.T) {
	p := newPool([]string{"x", "y", "z"})
	p2 := p.without("y")

	if p2.Len() != 2 {
		t.Fatalf("expected 2, got %d", p2.Len())
	}
	for _, e := range p2.Entries() {
		if e == "y" {
			t.Error("'y' should be removed")
		}
	}
}

func TestPool_Take(t *testing.T) {
	m := NewPoolManager(5)
	m.setPool(newPool([]string{"a", "b", "c", "d", "e"}))

	var idx atomic.Uint64
	ips := m.Take(3, &idx)
	if len(ips) != 3 {
		t.Fatalf("expected 3 IPs, got %d", len(ips))
	}
	t.Logf("took: %v", ips)
}

func TestPool_Take_MoreThanAvailable(t *testing.T) {
	m := NewPoolManager(5)
	m.setPool(newPool([]string{"a", "b"}))

	var idx atomic.Uint64
	ips := m.Take(10, &idx)
	if len(ips) != 2 {
		t.Fatalf("expected 2 IPs (capped), got %d", len(ips))
	}
}

func TestPool_RecordFailure_Removes(t *testing.T) {
	m := NewPoolManager(5)
	m.setPool(newPool([]string{"a", "b", "c"}))

	for i := 0; i < maxConsecutiveFailures; i++ {
		m.RecordFailure("a")
	}

	entries := m.PoolEntries()
	for _, e := range entries {
		if e == "a" {
			t.Error("node 'a' should be removed after max failures")
		}
	}
	if m.PoolLen() != 2 {
		t.Errorf("expected pool size 2, got %d", m.PoolLen())
	}
}

func TestPool_RecordFailure_Incremental(t *testing.T) {
	m := NewPoolManager(5)
	m.setPool(newPool([]string{"a", "b"}))

	m.RecordFailure("a")
	m.RecordFailure("a")

	// Not yet at threshold — node stays
	if m.PoolLen() != 2 {
		t.Errorf("expected 2, got %d", m.PoolLen())
	}
	p := m.getPool()
	if p.Failures("a") != 2 {
		t.Errorf("expected 2 failures, got %d", p.Failures("a"))
	}
}

func TestPool_RecordSuccess_Resets(t *testing.T) {
	m := NewPoolManager(5)
	m.setPool(newPool([]string{"a", "b"}))

	m.RecordFailure("a")
	m.RecordFailure("a")
	m.RecordSuccess("a")

	// Counter reset — one more failure should not remove
	m.RecordFailure("a")

	if m.PoolLen() != 2 {
		t.Error("node 'a' should still be in pool after success reset")
	}
	p := m.getPool()
	if p.Failures("a") != 1 {
		t.Errorf("expected 1 failure after reset+1, got %d", p.Failures("a"))
	}
}

func TestPool_CAS_Concurrent(t *testing.T) {
	m := NewPoolManager(10)
	m.setPool(newPool([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}))

	// Hammer RecordFailure and RecordSuccess from multiple goroutines
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(ip string) {
			for j := 0; j < 100; j++ {
				m.RecordFailure(CfNode(ip))
				m.RecordSuccess(CfNode(ip))
			}
			done <- struct{}{}
		}(string(rune('a' + i)))
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	// Just verify no panic/corruption — pool should still be valid
	p := m.getPool()
	if p == nil {
		t.Fatal("pool is nil after concurrent operations")
	}
	t.Logf("pool has %d entries after concurrent ops", p.Len())
}

func TestRefreshPool_SkipsWhenHealthy(t *testing.T) {
	m := NewPoolManager(5)
	m.setPool(newPool([]string{"a", "b", "c", "d"}))

	// 4/5 = 80% > 61.8%, should skip
	m.RefreshPool()
	if m.PoolLen() != 4 {
		t.Errorf("expected pool to remain at 4, got %d", m.PoolLen())
	}
}

func TestCSV_Persistence(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "nodes.csv")

	m := NewPoolManagerWithFile(5, csvPath)
	m.setPool(newPool([]string{"104.16.1.1", "104.16.2.2", "104.16.3.3"}))
	m.savePool(m.getPool())

	if _, err := os.Stat(csvPath); err != nil {
		t.Fatalf("CSV not created: %v", err)
	}

	m2 := NewPoolManagerWithFile(5, csvPath)
	if m2.PoolLen() != 3 {
		t.Fatalf("expected 3 nodes from CSV, got %d", m2.PoolLen())
	}
	entries := m2.PoolEntries()
	if entries[0] != "104.16.1.1" {
		t.Errorf("expected first node 104.16.1.1, got %s", entries[0])
	}
}

func TestCSV_PersistsOnRemove(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "nodes.csv")

	m := NewPoolManagerWithFile(5, csvPath)
	m.setPool(newPool([]string{"a", "b", "c"}))
	m.savePool(m.getPool())

	// Remove via 3 failures
	for i := 0; i < maxConsecutiveFailures; i++ {
		m.RecordFailure("b")
	}

	m2 := NewPoolManagerWithFile(5, csvPath)
	entries := m2.PoolEntries()
	for _, e := range entries {
		if e == "b" {
			t.Error("removed node 'b' should not be in CSV")
		}
	}
}
