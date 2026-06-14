# go-cloudflare-fastest-transport

Go library for Cloudflare IP optimization (优选 IP). Discovers the fastest Cloudflare edge nodes from your location and provides an `http.RoundTripper` that load-balances traffic across them with automatic failover.

Zero external dependencies. Lock-free pool management via immutable snapshots + CAS.

Also includes `cf-tunnel`, a TCP proxy CLI that routes HTTPS traffic through optimized CF nodes.

## Install

```bash
go get github.com/geektr-cloud/go-cloudflare-fastest-transport
```

## cf-tunnel CLI

A layer-4 TCP proxy that forwards connections to Cloudflare edge nodes with round-robin load balancing.

### Build & Run

```bash
go build -o cf-tunnel ./cmd/cf-tunnel

# Default: listen on port 443, pool of 20 nodes
sudo cf-tunnel

# Custom port and pool size
cf-tunnel --port 8443 --pool 30 --file my-nodes.csv
```

### Client Usage

```bash
# If listening on port 443:
curl --resolve 'mirs.uk:443:127.0.0.1' 'https://mirs.uk/path/to/file'

# If listening on a non-standard port (e.g. 8443), use --connect-to:
curl --connect-to 'mirs.uk:443:127.0.0.1:8443' 'https://mirs.uk/path/to/file'

# Download example
curl -o debian.iso --connect-to 'mirs.uk:443:127.0.0.1:8443' \
  'https://mirs.uk/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso'
```

Note: use `--connect-to` (not `--resolve`) with non-standard ports. `--resolve` puts the port in the HTTP Host header, which CF/origin servers reject.

## Library Usage

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "time"

    cft "github.com/geektr-cloud/go-cloudflare-fastest-transport"
)

func main() {
    // Create pool manager (20 IPs in reservoir, persisted to CSV)
    mgr := cft.NewPoolManagerWithFile(20, "cf-nodes.csv")

    // Or without persistence:
    // mgr := cft.NewPoolManager(20)

    // Discover nodes (skips if pool is >61.8% full)
    mgr.RefreshPool()

    // Create Transport with 3 upstream IPs in rotation
    transport := mgr.Transport(cft.TransportOptions{
        UpstreamCount:  3,
        EliminateDelay: 5 * time.Second,
    })

    // Use as http.Client transport
    client := &http.Client{Transport: transport}
    resp, err := client.Get("https://your-cf-site.com/api/data")
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
    body, _ := io.ReadAll(resp.Body)
    fmt.Println(string(body))
}
```

## Architecture

### Pool (Immutable, Lock-Free)

The `Pool` is an immutable value holding `[]string` entries and a `map[string]int` failure counter. All mutations produce a new `Pool` and swap it atomically via `CompareAndSwap`. No mutexes on the read path.

### PoolManager

- `Refresh()` — TCP ping ~6000 candidates → HTTP ping top 5×poolSize → store top poolSize
- `RefreshPool()` — skips if healthy nodes > 61.8% of poolSize
- `ForceRefreshPool()` — deduplicated via version check (concurrent calls coalesce)
- `Take(n, &index)` — returns n IPs round-robin; triggers background refresh when remaining < 2n
- `RecordFailure(node)` — CAS-increments failure; at 3 failures removes from pool + persists
- `RecordSuccess(node)` — CAS-resets failure counter

### Transport (`http.RoundTripper`)

- Maintains a small `UpstreamCount` subset for cache-friendly rotation
- Caches `*http.Transport` per upstream IP (TLS connection reuse)
- On upstream eviction, lazily refreshes from pool via `Take()`

### cf-tunnel (TCP Proxy)

- Pure layer-4: accepts TCP, dials CF edge :443, pipes bidirectionally
- Proper TCP half-close for clean teardown
- Only records failures (dial errors); no success tracking for TCP streams

## API

### CfNode

```go
type CfNode string

func (n CfNode) TCPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error)
func (n CfNode) HTTPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error)
func (n CfNode) SpeedTest(timeout time.Duration) (speed float64, err error)  // bytes/sec
```

### PoolManager

```go
func NewPoolManager(poolSize int) *PoolManager
func NewPoolManagerWithFile(poolSize int, path string) *PoolManager

func (m *PoolManager) Refresh() error
func (m *PoolManager) RefreshPool()
func (m *PoolManager) ForceRefreshPool()
func (m *PoolManager) Take(n int, index *atomic.Uint64) []string
func (m *PoolManager) RecordFailure(node CfNode)
func (m *PoolManager) RecordSuccess(node CfNode)
func (m *PoolManager) PoolEntries() []string
func (m *PoolManager) PoolLen() int
```

### Transport

```go
type TransportOptions struct {
    UpstreamCount  int           // IPs in rotation (limits cache pollution)
    EliminateDelay time.Duration // dial timeout
}

func (m *PoolManager) Transport(opts TransportOptions) *Transport
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error)
func (t *Transport) PoolSize() int
func (t *Transport) UpstreamSize() int
```

## CSV Format

```csv
ip
104.16.1.1
108.162.192.5
172.67.23.100
```

Simple one-column format. Failed IPs are removed permanently.

## Testing

```bash
go build ./...          # build all
go test ./...           # full suite with network tests (~80s)
go test -short ./...    # local-only tests (fast)
go test -race ./...     # with race detector
go vet ./...            # lint
```

## License

MIT
