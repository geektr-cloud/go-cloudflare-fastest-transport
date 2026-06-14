# go-cloudflare-fastest-transport

Go library for Cloudflare IP optimization (优选 IP). Automatically discovers the fastest Cloudflare edge nodes from your location and provides an `http.RoundTripper` that load-balances traffic across them with automatic failover.

Also includes `cf-tunnel`, a TCP proxy CLI that routes any HTTPS traffic through optimized CF nodes.

## Install

```bash
go get github.com/geektr-cloud/go-cloudflare-fastest-transport
```

## cf-tunnel CLI

A TCP-level proxy that forwards connections to Cloudflare edge nodes with round-robin load balancing.

### Build

```bash
go build -o cf-tunnel ./cmd/cf-tunnel
```

### Run

```bash
# Default: listen on port 443, discover 3 best nodes
sudo cf-tunnel

# Custom port (non-root)
cf-tunnel --port 8443

# Options
cf-tunnel --port 8443 --top 5 --file my-nodes.csv
```

### Usage

```bash
# If listening on port 443:
curl --resolve 'mirs.uk:443:127.0.0.1' 'https://mirs.uk/path/to/file'

# If listening on a non-standard port (e.g. 8443), use --connect-to:
curl --connect-to 'mirs.uk:443:127.0.0.1:8443' 'https://mirs.uk/path/to/file'

# Download example
curl -o debian.iso --connect-to 'mirs.uk:443:127.0.0.1:8443' \
  'https://mirs.uk/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso'
```

Note: use `--connect-to` (not `--resolve`) with non-standard ports. `--resolve` causes curl to include the port in the HTTP Host header, which CF/origin servers may reject.

### Persistence

Node discovery results are saved to a CSV file (`cf-nodes.csv` by default). On next startup, if enough active nodes exist in the file, the expensive Refresh step is skipped.

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
    // With file persistence (skips Refresh on subsequent runs)
    nodeSet := cft.NewCfNodeSetWithFile("cf-nodes.csv")

    // Or without persistence
    // nodeSet := cft.NewCfNodeSet()

    if err := nodeSet.Refresh(10); err != nil {
        panic(err)
    }

    transport := nodeSet.Transport(cft.FilterOptions{
        EliminateDelay: 500 * time.Millisecond,
        TargetDelay:    200 * time.Millisecond,
        EliminateSpeed: 1 * 1024 * 1024,  // 1 MB/s
        TargetSpeed:    5 * 1024 * 1024,   // 5 MB/s
        TopN:           3,
    })

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

## How It Works

### Node Discovery (`Refresh`)

1. Generates ~6000 candidate IPs from Cloudflare's published IPv4 CIDR ranges
2. TCP ping all candidates concurrently (200 goroutines) to find reachable nodes
3. HTTP ping the top 5×N nodes to measure real latency
4. Returns the best N nodes, respecting any existing ban list

### Node Selection (`Filter`)

Tests nodes against speed/delay thresholds:
- Nodes below `EliminateSpeed` or above `EliminateDelay` are banned for 1 hour
- Nodes above `TargetSpeed` or below `TargetDelay` are accepted immediately
- If not enough nodes meet targets, applies a golden-ratio (0.618) fallback threshold

### Transport (`http.RoundTripper`)

- Round-robin load balancing across selected nodes
- Tracks consecutive failures per node
- 3 consecutive failures → ban for 1 hour, remove from pool
- When the last node fails, automatically re-filters for fresh nodes

### cf-tunnel (TCP proxy)

- Pure layer-4 forwarding — no TLS termination, no protocol inspection
- Accepts TCP connections, dials a CF edge IP on port 443, pipes bytes bidirectionally
- Proper TCP half-close handling for clean connection teardown

## API

### CfNode

```go
type CfNode string

func (n CfNode) TCPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error)
func (n CfNode) HTTPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error)
func (n CfNode) SpeedTest(timeout time.Duration) (speed float64, err error)
```

### CfNodeSet

```go
func NewCfNodeSet() *CfNodeSet
func NewCfNodeSetWithFile(path string) *CfNodeSet
func (s *CfNodeSet) Refresh(topN int) error
func (s *CfNodeSet) Filter(opts FilterOptions) []string
func (s *CfNodeSet) Transport(opts FilterOptions) *Transport
func (s *CfNodeSet) Ban(node CfNode)
func (s *CfNodeSet) List() []CfNodeStatus
```

### FilterOptions

```go
type FilterOptions struct {
    EliminateSpeed float64       // bytes/s — below this, ban
    TargetSpeed    float64       // bytes/s — above this, accept
    EliminateDelay time.Duration // above this, ban
    TargetDelay    time.Duration // below this, accept
    TopN           int
}
```

## CSV Format

```csv
ip,ban_expire
104.16.1.1,0
192.0.2.1,2026-06-13T12:00:00Z
```

`ban_expire` is `0` for active nodes, or an RFC3339 timestamp.

## Testing

```bash
go test ./...           # full suite with real network tests (~80s per phase)
go test -short ./...    # local-only tests (fast)
go vet ./...            # lint
```

## License

MIT
