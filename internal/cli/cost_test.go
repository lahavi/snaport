package cli

import (
	"math"
	"testing"
)

func TestEstimateCostRequestsOnly(t *testing.T) {
	// 40 GiB of 512 KiB blocks = 81,920 get requests + 9 list pages.
	e := estimateCost(9, 81920, 40*1024*1024*1024, 0)
	if e.EgressGB != 0 || e.EgressCostUSD != 0 {
		t.Fatalf("egress should be disabled: %+v", e)
	}
	want := (float64(9) + float64(81920)) / 1000 * 0.003
	if math.Abs(e.RequestCostUSD-want) > 1e-12 {
		t.Fatalf("request cost %v, want %v", e.RequestCostUSD, want)
	}
	if e.TotalUSD != e.RequestCostUSD {
		t.Fatalf("total %v != request %v", e.TotalUSD, e.RequestCostUSD)
	}
}

const gb = 1e9

func TestEstimateCostWithEgress(t *testing.T) {
	e := estimateCost(1, 1000, 2*gb, 0.09)
	want := 0.003 + 1.0/1000*0.003 // 1,000 gets + 1 list
	if math.Abs(e.RequestCostUSD-want) > 1e-12 {
		t.Fatalf("request cost %v, want %v", e.RequestCostUSD, want)
	}
	if math.Abs(e.EgressGB-2) > 1e-9 {
		t.Fatalf("egress %v GB, want 2", e.EgressGB)
	}
	if math.Abs(e.EgressCostUSD-0.18) > 1e-9 {
		t.Fatalf("egress cost %v, want 0.18", e.EgressCostUSD)
	}
	if e.TotalUSD <= e.RequestCostUSD || e.TotalUSD <= e.EgressCostUSD {
		t.Fatalf("total %v not the sum", e.TotalUSD)
	}
}

func TestEstimateCostZeroAndNegative(t *testing.T) {
	e := estimateCost(0, 0, 0, 0.09)
	if e.TotalUSD != 0 || e.EgressGB != 0 {
		t.Fatalf("empty estimate: %+v", e)
	}
	// Negative egress rate is treated as disabled, not as a credit.
	if e := estimateCost(1, 1, gb, -1); e.EgressCostUSD != 0 {
		t.Fatalf("negative rate must not produce a credit: %+v", e)
	}
}

func TestUsdFormatting(t *testing.T) {
	cases := map[float64]string{
		0:        "$0",
		0.000303: "$0.0003",
		0.09:     "$0.090",
		0.5:      "$0.500",
		3.996:    "$4.00",
		13.5:     "$13.50",
	}
	for in, want := range cases {
		if got := usd(in); got != want {
			t.Errorf("usd(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestHumanCount(t *testing.T) {
	cases := map[int64]string{
		0:     "0",
		999:   "999",
		1000:  "1,000",
		81920: "81,920",
		1234:  "1,234",
	}
	for in, want := range cases {
		if got := humanCount(in); got != want {
			t.Errorf("humanCount(%d) = %q, want %q", in, got, want)
		}
	}
}
