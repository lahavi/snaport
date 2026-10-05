package cli

import (
	"fmt"
	"os"
	"sync"
	"time"

	"snaport/internal/dl"
)

// progressRenderer draws a single-line progress display on stderr. When
// stderr is not a terminal it prints sparse periodic lines instead, so
// logs stay readable.
type progressRenderer struct {
	mu       sync.Mutex
	quiet    bool
	isTTY    bool
	last     time.Time
	start    time.Time
	lastLine string
}

func newProgress(quiet bool) *progressRenderer {
	isTTY := false
	if st, err := os.Stderr.Stat(); err == nil {
		isTTY = st.Mode()&os.ModeCharDevice != 0
	}
	return &progressRenderer{quiet: quiet, isTTY: isTTY, start: time.Now()}
}

// Update renders one progress snapshot (rate-limited to ~4/s).
func (p *progressRenderer) Update(u dl.Update) {
	if p.quiet {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if !p.isTTY {
		// Plain logs: one line every 10 seconds.
		if now.Sub(p.last) < 10*time.Second {
			return
		}
		p.last = now
		fmt.Fprintf(os.Stderr, "%s\n", formatProgress(u, now.Sub(p.start), false))
		return
	}
	if now.Sub(p.last) < 250*time.Millisecond && u.Phase != dl.PhaseVerify && u.Phase != dl.PhaseCompress {
		return
	}
	p.last = now
	line := formatProgress(u, now.Sub(p.start), true)
	if line == p.lastLine {
		return
	}
	p.lastLine = line
	fmt.Fprint(os.Stderr, "\r"+line)
}

// Finish terminates the progress line (if any).
func (p *progressRenderer) Finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isTTY && p.lastLine != "" {
		fmt.Fprint(os.Stderr, "\r"+spaces(len(p.lastLine))+"\r")
		p.lastLine = ""
	}
}

func spaces(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return string(b)
}

var phaseLabels = map[string]string{
	dl.PhaseList:     "listing blocks",
	dl.PhaseDownload: "downloading",
	dl.PhaseVerify:   "verifying",
	dl.PhaseCompress: "compressing",
}

func formatProgress(u dl.Update, elapsed time.Duration, tty bool) string {
	label := phaseLabels[u.Phase]
	if label == "" {
		label = u.Phase
	}
	switch u.Phase {
	case dl.PhaseList:
		return fmt.Sprintf("[%s] listing snapshot blocks...", label)
	case dl.PhaseDownload:
		pct := 0.0
		if u.BlocksTotal > 0 {
			pct = float64(u.BlocksDone) / float64(u.BlocksTotal) * 100
		}
		rate := ""
		if u.BytesDone > 0 && elapsed > 0 {
			rate = fmt.Sprintf(" %s/s", humanBytes(float64(u.BytesDone)/elapsed.Seconds()))
		}
		eta := ""
		if u.BlocksDone > 0 && u.BlocksDone < u.BlocksTotal && u.BytesDone > 0 && u.BytesTotal > u.BytesDone {
			remain := time.Duration(float64(elapsed) / float64(u.BytesDone) * float64(u.BytesTotal-u.BytesDone))
			eta = fmt.Sprintf(" eta %s", remain.Round(time.Second))
		}
		throttle := ""
		if u.Concurrency > 0 {
			throttle = fmt.Sprintf(" [parallel %d]", u.Concurrency)
		}
		return fmt.Sprintf("[%-12s] %d/%d blocks (%.1f%%)%s%s%s",
			label, u.BlocksDone, u.BlocksTotal, pct, rate, eta, throttle)
	default: // verify / compress
		pct := 0.0
		if u.BytesTotal > 0 {
			pct = float64(u.BytesDone) / float64(u.BytesTotal) * 100
		}
		return fmt.Sprintf("[%-12s] %s / %s (%.1f%%)",
			label, humanBytes(float64(u.BytesDone)), humanBytes(float64(u.BytesTotal)), pct)
	}
}

// humanBytes formats bytes in binary units.
func humanBytes(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", b/(1<<10))
	default:
		return fmt.Sprintf("%d B", int64(b))
	}
}
