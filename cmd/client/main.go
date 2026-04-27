package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sartoopjj/vpn-over-github/client"
	"github.com/sartoopjj/vpn-over-github/shared"
)

// Version is set at build time via -ldflags "-X main.Version=x.y.z".
var Version = "dev"

func main() {
	noTUI := flag.Bool("no-tui", false, "disable terminal UI; logs go to stdout")

	client.Version = Version // propagate build-time version into library
	cfg, err := client.ParseFlags()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// In TUI mode, slog goes to a temp file so it doesn't corrupt the display.
	var logFilePath string
	if !*noTUI {
		logFilePath = fmt.Sprintf("%s/gh-tunnel-client.log", os.TempDir())
		f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			slog.SetDefault(slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil)))
			logFilePath = ""
		} else {
			slog.SetDefault(slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})))
		}
	} else {
		client.SetupLogging(cfg)
	}

	slog.Info("gh-tunnel-client starting",
		"version", Version,
		"socks_listen", cfg.SOCKS.Listen,
		"tokens", len(cfg.GitHub.Tokens),
		"encryption", cfg.Encryption.Algorithm,
	)

	rl := client.NewRateLimiter(tokenStrings(cfg.GitHub.Tokens), cfg)

	clients := make(map[int]shared.Transport, len(cfg.GitHub.Tokens))
	for i, tc := range cfg.GitHub.Tokens {
		transport := tc.Transport
		if transport == "" {
			transport = "git"
		}
		switch transport {
		case "git":
			rc, err := shared.NewGitSmartHTTPClient(tc.Token, tc.Repo)
			if err != nil {
				slog.Error("git transport init failed", "error", err, "token_index", i)
				os.Exit(1)
			}
			clients[i] = rc
			slog.Info("token transport", "index", i, "transport", "git", "repo", tc.Repo)
		default: // "gist"
			clients[i] = shared.NewGitHubGistClient(tc.Token, &http.Client{Timeout: cfg.GitHub.APITimeout})
			slog.Info("token transport", "index", i, "transport", "gist")
		}
	}

	manager, err := client.NewMuxClient(context.Background(), cfg, rl, clients)
	if err != nil {
		slog.Error("mux client init failed", "error", err)
		os.Exit(1)
	}
	socks := client.NewSOCKSServer(cfg.SOCKS.Listen, manager, cfg.SOCKS.Timeout, cfg.SOCKS.BufferSize)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if !*noTUI {
		go func() {
			if err := socks.ListenAndServe(ctx); err != nil {
				slog.Error("socks5 server error", "error", err)
			}
		}()
		if err := client.RunTUI(ctx, stop, manager, rl, cfg, Version, logFilePath); err != nil {
			slog.Error("TUI error", "error", err)
		}
	} else {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					rl.LogStatus()
				}
			}
		}()
		if err := socks.ListenAndServe(ctx); err != nil {
			slog.Error("socks5 server error", "error", err)
		}
	}

	slog.Info("shutting down, closing all tunnel connections")
	manager.CloseAll(context.Background())
	slog.Info("shutdown complete")
}

// tokenStrings extracts the raw token strings for the rate-limiter.
func tokenStrings(tcs []client.TokenConfig) []string {
	out := make([]string, len(tcs))
	for i, tc := range tcs {
		out[i] = tc.Token
	}
	return out
}
