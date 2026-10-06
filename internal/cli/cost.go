package cli

import "fmt"

// EBS Direct API request pricing (published AWS rates). Verify against
// current pricing at https://aws.amazon.com/ebs/pricing/ before relying
// on estimates for budget decisions.
const (
	usdPerKListRequests = 0.003 // ListSnapshotBlocks, per 1,000 requests
	usdPerKGetRequests  = 0.003 // GetSnapshotBlock, per 1,000 requests
)

// costEstimate is a pre-flight cost projection for one planned download.
// It counts only the happy path: one GetSnapshotBlock per allocated
// block plus the listing pages already observed during the dry run.
// Throttled retries, checksum-mismatch refetches and token-refresh
// re-lists add a marginal number of requests and are not included.
type costEstimate struct {
	ListRequests   int64
	GetRequests    int64
	RequestCostUSD float64
	EgressGB       float64
	EgressCostUSD  float64
	TotalUSD       float64
}

// estimateCost projects the cost of downloading. egressPerGB is the
// internet data-transfer-out rate in USD per decimal GB for your region
// (0 disables the egress component, e.g. when running inside AWS).
func estimateCost(listRequests, getRequests int, dataBytes int64, egressPerGB float64) costEstimate {
	e := costEstimate{
		ListRequests: int64(listRequests),
		GetRequests:  int64(getRequests),
	}
	e.RequestCostUSD = float64(e.ListRequests)/1000*usdPerKListRequests +
		float64(e.GetRequests)/1000*usdPerKGetRequests
	if egressPerGB > 0 && dataBytes > 0 {
		e.EgressGB = float64(dataBytes) / 1e9
		e.EgressCostUSD = e.EgressGB * egressPerGB
	}
	e.TotalUSD = e.RequestCostUSD + e.EgressCostUSD
	return e
}

// usd formats a small USD amount without losing precision.
func usd(v float64) string {
	switch {
	case v == 0:
		return "$0"
	case v < 0.01:
		return fmt.Sprintf("$%.4f", v)
	case v < 1:
		return fmt.Sprintf("$%.3f", v)
	default:
		return fmt.Sprintf("$%.2f", v)
	}
}
