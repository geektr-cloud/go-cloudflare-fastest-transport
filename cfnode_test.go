package cftransport

import (
	"testing"
	"time"
)

const testNode CfNode = "104.16.132.229" // known reachable CF IP

// speedTestNode uses an IP that resolves for speed.cloudflare.com
const speedTestNode CfNode = "162.159.140.220"

func TestCfNode_TCPPing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	delay, lossRate, err := testNode.TCPPing(2 * time.Second)
	if err != nil {
		t.Fatalf("TCPPing failed: %v", err)
	}
	if delay <= 0 {
		t.Errorf("expected positive delay, got %v", delay)
	}
	if lossRate < 0 || lossRate > 1 {
		t.Errorf("loss rate out of range [0,1]: %f", lossRate)
	}
	t.Logf("TCPPing: delay=%v, lossRate=%.2f", delay, lossRate)
}

func TestCfNode_TCPPing_InvalidHost(t *testing.T) {
	node := CfNode("192.0.2.1") // TEST-NET, should be unreachable
	_, lossRate, err := node.TCPPing(500 * time.Millisecond)
	if err == nil {
		t.Fatal("expected error for unreachable host")
	}
	if lossRate != 1.0 {
		t.Errorf("expected 100%% loss, got %f", lossRate)
	}
}

func TestCfNode_HTTPPing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	delay, lossRate, err := testNode.HTTPPing(5 * time.Second)
	if err != nil {
		t.Fatalf("HTTPPing failed: %v", err)
	}
	if delay <= 0 {
		t.Errorf("expected positive delay, got %v", delay)
	}
	if lossRate < 0 || lossRate > 1 {
		t.Errorf("loss rate out of range [0,1]: %f", lossRate)
	}
	t.Logf("HTTPPing: delay=%v, lossRate=%.2f", delay, lossRate)
}

func TestCfNode_SpeedTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	speed, err := speedTestNode.SpeedTest(5 * time.Second)
	if err != nil {
		t.Fatalf("SpeedTest failed: %v", err)
	}
	if speed <= 0 {
		t.Errorf("expected positive speed, got %f", speed)
	}
	t.Logf("SpeedTest: speed=%.2f bytes/s (%.2f MB/s)", speed, speed/1024/1024)
}
