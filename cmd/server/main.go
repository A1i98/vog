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

	"github.com/sartoopjj/vpn-over-github/server"
	"github.com/sartoopjj/vpn-over-github/shared"
)

// Version is set at build time via -ldflags "-X main.Version=x.y.z".
var Version = "dev"

func main() {
	configPath := flag.String("config", "/opt/gh-tunnel/server-config.yaml", "Path to server config file")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("gh-tunnel-server %s\n", Version)
		os.Exit(0)
	}

	cfg, err := server.LoadServerConfig(*configPath)
	if err != nil {
		slog.Error("invalid server configuration", "error", err, "path", *configPath)
		os.Exit(1)
	}

	server.SetupLogging(cfg)

	slog.Info("gh-tunnel-server starting",
		"version", Version,
		"tokens", len(cfg.GitHub.Tokens),
		"fetch_interval", cfg.GitHub.FetchInterval.String(),
		"encryption", cfg.Encryption.Algorithm,
	)

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

	listener := server.NewChannelListener(cfg, clients, cfg.GitHub.Tokens)
	cleanup := server.NewCleanupDaemon(cfg, clients)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start cleanup daemon
	if cfg.Cleanup.Enabled {
		go cleanup.Run(ctx)
	}

	// Start channel listener (blocks)
	if err := listener.Run(ctx); err != nil {
		slog.Error("channel listener error", "error", err)
	}

	slog.Info("server shutdown complete")
}
