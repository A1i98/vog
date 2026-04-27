package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// tuiTickMsg is sent every 500 ms to refresh the dashboard.
type tuiTickMsg struct{}

// tuiQuitMsg is sent by the context-watcher goroutine when the app shuts down.
type tuiQuitMsg struct{}

// connRates holds the per-second transfer rate for one connection.
type connRates struct {
	upRate   int64 // bytes/sec
	downRate int64 // bytes/sec
}

// tuiModel is the bubbletea model for the client dashboard.
type tuiModel struct {
	manager   *MuxClient
	rl        *RateLimiter
	cfg       *Config
	version   string
	logFile   string // path to log file in TUI mode, empty if none
	startTime time.Time
	width     int

	conns     []ConnSnapshot
	tokens    []TokenSnapshot
	prevConns map[string]ConnSnapshot
	rates     map[string]connRates
	lastTick  time.Time
}

func newTUIModel(manager *MuxClient, rl *RateLimiter, cfg *Config, version, logFile string) tuiModel {
	return tuiModel{
		manager:   manager,
		rl:        rl,
		cfg:       cfg,
		version:   version,
		logFile:   logFile,
		startTime: time.Now(),
		lastTick:  time.Now(),
		width:     80,
		prevConns: make(map[string]ConnSnapshot),
		rates:     make(map[string]connRates),
	}
}

func (m tuiModel) Init() tea.Cmd {
	return tuiTickCmd()
}

func tuiTickCmd() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(time.Time) tea.Msg {
		return tuiTickMsg{}
	})
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width

	case tuiTickMsg:
		now := time.Now()
		elapsed := now.Sub(m.lastTick).Seconds()
		if elapsed < 0.001 {
			elapsed = 0.001
		}

		newConns := m.manager.Snapshot()
		newRates := make(map[string]connRates, len(newConns))
		newPrev := make(map[string]ConnSnapshot, len(newConns))
		for _, c := range newConns {
			if prev, ok := m.prevConns[c.ConnID]; ok {
				up := c.BytesUp - prev.BytesUp
				dn := c.BytesDown - prev.BytesDown
				if up < 0 {
					up = 0
				}
				if dn < 0 {
					dn = 0
				}
				newRates[c.ConnID] = connRates{
					upRate:   int64(float64(up) / elapsed),
					downRate: int64(float64(dn) / elapsed),
				}
			}
			newPrev[c.ConnID] = c
		}
		m.conns = newConns
		m.rates = newRates
		m.prevConns = newPrev
		m.lastTick = now
		m.tokens = m.rl.RateSnapshot(m.cfg.RateLimit.MaxRequestsPerHour, m.cfg.GitHub.Tokens)
		return m, tuiTickCmd()

	case tuiQuitMsg:
		return m, tea.Quit

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m tuiModel) View() string {
	cw := m.width
	if cw > 90 {
		cw = 90
	}
	if cw < 60 {
		cw = 60
	}
	sep := strings.Repeat("─", cw)

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("86"))
	headStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255"))
	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	connStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("82"))
	dstStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	warnStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	barOK := lipgloss.NewStyle().Foreground(lipgloss.Color("82"))
	barWarn := lipgloss.NewStyle().Foreground(lipgloss.Color("214"))

	var b strings.Builder

	uptime := time.Since(m.startTime).Round(time.Second)
	b.WriteString(titleStyle.Render("gh-tunnel-client " + m.version))
	b.WriteString(dimStyle.Render(fmt.Sprintf("  uptime %s  SOCKS5 %s", uptime, m.cfg.SOCKS.Listen)))
	b.WriteByte('\n')
	b.WriteString(sep + "\n")

	// Count active conns per token. The total is the lifetime counter from
	// the mux (includes bytes from already-closed conns).
	perToken := make(map[int]int)
	for _, c := range m.conns {
		perToken[c.TokenIdx]++
	}
	totalUp, totalDown := m.manager.TotalBytes()
	b.WriteString(headStyle.Render("Connections"))
	b.WriteString(dimStyle.Render(fmt.Sprintf(" (%d active)", len(m.conns))))
	b.WriteString(dimStyle.Render(fmt.Sprintf("  ↑ %s  ↓ %s total", tuiFmtBytes(totalUp), tuiFmtBytes(totalDown))))
	if len(perToken) > 0 {
		b.WriteString(dimStyle.Render("  ["))
		sep2 := ""
		for i, t := range m.tokens {
			if n, ok := perToken[i]; ok {
				b.WriteString(dimStyle.Render(sep2))
				b.WriteString(dimStyle.Render(fmt.Sprintf("token%d/%s:%d", i, t.Transport, n)))
				sep2 = "  "
			}
		}
		b.WriteString(dimStyle.Render("]"))
	}
	b.WriteByte('\n')
	b.WriteString(dimStyle.Render(tuiPad("CONN-ID", 18) + "  " + tuiPad("VIA", 5) + "  " + tuiPad("DESTINATION", 22) + "  " + tuiPad("↑ UP", 18) + "  " + "↓ DOWN"))
	b.WriteByte('\n')
	if len(m.conns) == 0 {
		b.WriteString(dimStyle.Render("  (none)"))
		b.WriteByte('\n')
	} else {
		for _, c := range m.conns {
			r := m.rates[c.ConnID]
			upStr := fmt.Sprintf("%s (%s/s)", tuiFmtBytes(c.BytesUp), tuiFmtBytes(r.upRate))
			dnStr := fmt.Sprintf("%s (%s/s)", tuiFmtBytes(c.BytesDown), tuiFmtBytes(r.downRate))
			via := tuiPad(c.Transport, 5)
			b.WriteString(connStyle.Render(tuiPad(tuiShortID(c.ConnID), 18)))
			b.WriteString("  ")
			b.WriteString(dimStyle.Render(via))
			b.WriteString("  ")
			b.WriteString(dstStyle.Render(tuiPad(tuiShortDst(c.Dst), 22)))
			b.WriteString("  ")
			b.WriteString(connStyle.Render(tuiPad(upStr, 18)))
			b.WriteString("  ")
			b.WriteString(connStyle.Render(dnStr))
			b.WriteByte('\n')
		}
	}
	b.WriteString(sep + "\n")

	b.WriteString(headStyle.Render("Token Quota"))
	b.WriteByte('\n')

	const barWidth = 24
	tokenIndent := strings.Repeat(" ", 26)

	for _, t := range m.tokens {
		isGit := t.Transport == "git" || t.Transport == ""
		name := tuiPad(t.MaskedToken, 16)
		kind := tuiPad(t.Transport, 4)
		linePrefix := "  " + dimStyle.Render(name) + "  " + dimStyle.Render(kind) + "  "
		apiStr := dimStyle.Render(fmt.Sprintf("  calls:%d", t.TotalAPICalls))

		if isGit {
			b.WriteString(linePrefix)
			b.WriteString(dimStyle.Render("git  no REST quota · ~25 concurrent · bandwidth-throttled"))
			b.WriteString(apiStr)
			if !t.BackoffUntil.IsZero() && time.Now().Before(t.BackoffUntil) {
				b.WriteString("  ")
				b.WriteString(warnStyle.Render("⏸ " + time.Until(t.BackoffUntil).Round(time.Second).String()))
			}
			b.WriteByte('\n')
			continue
		}

		// renderBar writes one labelled progress bar row.
		renderBar := func(pfx, label string, used, total int, warnAt float64) {
			pct := 0.0
			if total > 0 {
				pct = float64(used) / float64(total)
			}
			filled := int(pct * float64(barWidth))
			if filled > barWidth {
				filled = barWidth
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
			bStyle := barOK
			if pct > warnAt {
				bStyle = barWarn
			}
			remaining := total - used
			if remaining < 0 {
				remaining = 0
			}
			b.WriteString(pfx)
			b.WriteString(dimStyle.Render(tuiPad(label, 9)))
			b.WriteString("  ")
			b.WriteString(bStyle.Render(bar))
			b.WriteString("  ")
			b.WriteString(headStyle.Render(tuiPad(fmt.Sprintf("%d/%d", remaining, total), 9)))
		}

		restUsed := t.Total - t.Remaining
		if restUsed < 0 {
			restUsed = 0
		}

		// Row 1: REST primary quota (5000/hr)
		renderBar(linePrefix, "REST r/h", restUsed, t.Total, 0.80)
		b.WriteString(apiStr)
		if !t.BackoffUntil.IsZero() && time.Now().Before(t.BackoffUntil) {
			b.WriteString("  ")
			b.WriteString(warnStyle.Render("⏸ " + time.Until(t.BackoffUntil).Round(time.Second).String()))
		}
		b.WriteByte('\n')

		// Row 2: secondary write/min
		renderBar(tokenIndent, "WRITE/min", t.WritesPerMin, 80, 0.80)
		b.WriteByte('\n')

		// Row 3: secondary write/hr
		renderBar(tokenIndent, "WRITE/hr", t.WritesPerHour, 500, 0.80)
		b.WriteByte('\n')
	}

	b.WriteString(sep + "\n")
	footer := "q  quit"
	if m.logFile != "" {
		footer += "  │  logs: " + m.logFile
	}
	b.WriteString(dimStyle.Render(footer))
	b.WriteByte('\n')

	return b.String()
}

// tuiPad pads or truncates s to exactly n visible characters.
func tuiPad(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	return s + strings.Repeat(" ", n-len(s))
}

func tuiShortID(id string) string {
	if len(id) <= 18 {
		return id
	}
	return id[:8] + "…" + id[len(id)-8:]
}

func tuiShortDst(dst string) string {
	if len(dst) <= 22 {
		return dst
	}
	return dst[:19] + "…"
}

func tuiFmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fK", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// RunTUI starts the bubbletea dashboard. logFile is shown in the footer; pass "" to omit.
// Blocks until the user presses q/Ctrl+C or ctx is cancelled.
func RunTUI(ctx context.Context, cancel context.CancelFunc, manager *MuxClient, rl *RateLimiter, cfg *Config, version, logFile string) error {
	m := newTUIModel(manager, rl, cfg, version, logFile)
	p := tea.NewProgram(m, tea.WithAltScreen())
	go func() {
		<-ctx.Done()
		p.Send(tuiQuitMsg{})
	}()
	_, err := p.Run()
	cancel() // ensure context is cancelled when TUI exits (e.g. user pressed q)
	return err
}
