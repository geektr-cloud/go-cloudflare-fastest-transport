package cftransport

import (
	"math/rand"
	"net"
	"strings"
)

// Cloudflare IPv4 CIDR ranges.
var cfIPv4CIDRs = []string{
	"173.245.48.0/20",
	"103.21.244.0/22",
	"103.22.200.0/22",
	"103.31.4.0/22",
	"141.101.64.0/18",
	"108.162.192.0/18",
	"190.93.240.0/20",
	"188.114.96.0/20",
	"197.234.240.0/22",
	"198.41.128.0/17",
	"162.158.0.0/15",
	"104.16.0.0/12",
	"172.64.0.0/17",
	"172.64.128.0/18",
	"172.64.192.0/19",
	"172.64.224.0/22",
	"172.64.229.0/24",
	"172.64.230.0/23",
	"172.64.232.0/21",
	"172.64.240.0/21",
	"172.64.248.0/21",
	"172.65.0.0/16",
	"172.66.0.0/16",
	"172.67.0.0/16",
	"131.0.72.0/22",
}

// generateCandidateIPs generates one random IP from each /24 subnet
// within the Cloudflare CIDR ranges.
func generateCandidateIPs() []CfNode {
	var nodes []CfNode
	for _, cidr := range cfIPv4CIDRs {
		nodes = append(nodes, ipsFromCIDR(cidr)...)
	}
	return nodes
}

// ipsFromCIDR generates candidate IPs from a single CIDR range.
// For each /24 block within the range, one random IP is picked.
func ipsFromCIDR(cidr string) []CfNode {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil
	}

	maskOnes, _ := ipNet.Mask.Size()
	ip := make(net.IP, 4)
	copy(ip, ipNet.IP.To4())

	var nodes []CfNode

	if maskOnes >= 24 {
		// Single /24 or smaller — pick one random IP from last octet range
		minIP := ip[3] & ipNet.Mask[3]
		mask4 := ^ipNet.Mask[3]
		hosts := int(mask4)
		if hosts == 0 {
			// /32 — single IP
			nodes = append(nodes, CfNode(ip.String()))
		} else {
			ip[3] = minIP + byte(rand.Intn(hosts+1))
			nodes = append(nodes, CfNode(ip.String()))
		}
		return nodes
	}

	// Larger than /24 — iterate through /24 blocks
	for ipNet.Contains(ip) {
		// Pick one random IP in this /24
		last := byte(rand.Intn(256))
		candidate := net.IPv4(ip[0], ip[1], ip[2], last).To4()
		nodes = append(nodes, CfNode(candidate.String()))

		// Advance to next /24
		ip[2]++
		if ip[2] == 0 {
			ip[1]++
			if ip[1] == 0 {
				ip[0]++
			}
		}
	}

	return nodes
}

// shuffleNodes randomizes the order of a CfNode slice in-place.
func shuffleNodes(nodes []CfNode) {
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
}

// isIPv4 checks if a string is an IPv4 address.
func isIPv4(ip string) bool {
	return strings.Contains(ip, ".") && !strings.Contains(ip, ":")
}
