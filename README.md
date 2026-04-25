# VPN Over GitHub

> **⚠️ WARNING: For educational / authorized research use only.**
> This likely violates [GitHub's Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service).
> GitHub can read all Gist/Repo content. The default XOR cipher is **not** cryptographically secure.
> You are solely responsible for all consequences of using this tool.

---

## Does it actually work?

**Sort of, but it's slow and has real limits.**

- Every packet is a round-trip to `api.github.com` or `github.com` → **100–500 ms latency per request**.
- **`gist` transport**: bound by GitHub's **secondary rate limit of ~500 content-generating writes/hr per account**.
- **`git` transport** (recommended for high traffic): push/pull over git Smart HTTP uses a **completely separate rate-limit pool** with no hard published per-hour write ceiling. Far more headroom.
- New GitHub accounts have much lower REST limits (~100 req/hr). The `git` transport is unaffected.
- Interactive sessions (SSH, light browsing) work on either transport. Video or large downloads will exhaust the `gist` transport quota fast.

---

## Overview

Tunnels TCP connections through GitHub. Supports two transports:

| Transport | How it works | Required PAT scope | Rate limit |
|---|---|---|---|
| **git** (default) | Packets stored in a private repo via git push/pull over HTTPS | `repo` | No REST quota; ~25 concurrent connections; bandwidth-throttled |
| **gist** | Packets stored in private GitHub Gists via REST API | `gist` | ~500 writes/hr per account (REST secondary limit) |

The `gist` transport talks to `api.github.com`.  
The `git` transport talks to `github.com` (git Smart HTTP) — a different rate-limit pool.

```
App → SOCKS5 (client) → GitHub (gist or git) → gh-tunnel-server → Internet
```

### Rate limit facts

**`gist` transport** enforces three independent GitHub buckets:
- **REST r/h** — 5,000 authenticated REST requests/hr per token (primary quota)
- **WRITE/min** — 80 content-generating writes (PATCH/POST) per minute (secondary)
- **WRITE/hr** — 500 content-generating writes per hour (secondary)

All three are shared **per GitHub account** (not per token). Multiple PATs from the same account share one pool.
PATs from **different accounts** each get their own pool — the only way to multiply `gist` capacity.

**`git` transport** uses GitHub's git Smart HTTP protocol.
- No REST quota applies — entirely separate rate-limit infrastructure.
- GitHub limits: ~25 concurrent git connections per user; bandwidth-throttled (no hard per-hour push count).
- Switching from `gist` to `git` removes the 500 writes/hr bottleneck and eliminates the REST quota concern.

---

## Quick Start

### Prerequisites
- Go 1.21+
- GitHub PAT with `gist` scope (gist transport) or `repo` scope (git transport)
- Same token(s) and encryption setting on both client and server

### Client
```bash
make build-client
./build/gh-tunnel-client -config client_config.yaml          # TUI dashboard (default)
./build/gh-tunnel-client --no-tui -config client_config.yaml # plain logs
curl -x socks5h://127.0.0.1:1080 https://api.ipify.org
```

### Server
```bash
make build-server
./build/gh-tunnel-server -config server_config.yaml
```

---

## Transport Setup


### Configuration (per-token, required)

Copy `example_client_config.yaml` or `example_server_config.yaml` and edit. The config **must** be an array of token objects, each specifying its own `token`, and optionally `transport` and `repo` fields:

```yaml
github:
  tokens:
    # Minimal entry
    - token: "ghp_your_first_token_here"
      transport: "git"
      repo: "yourusername/tunnel-data"

    # Explicit gist transport
    - token: "ghp_second_token"
      transport: "gist"

    # Git Smart HTTP transport (no REST rate limit, recommended for high traffic)
    - token: "ghp_git_token"
      transport: "git"
      repo: "yourusername/tunnel-data"

    # Mix tokens from different accounts for more capacity
    - token: "ghp_account1_token"
      transport: "git"
      repo: "account1/tunnel-repo"
    - token: "ghp_account2_token"
      transport: "git"
      repo: "account2/tunnel-repo"
```

**Notes:**
- Each token object must have a `token` field.
- `transport` and `repo` are per-token. If omitted, `transport` defaults to `git`. `repo` is required for `git` transport.
- Only tokens from **different GitHub accounts** multiply your write capacity. Tokens from the same account share one rate-limit pool.

#### Example: Gist transport

```yaml
github:
  tokens:
    - token: "ghp_yourtoken"
      transport: "gist"
```

#### Example: Git Smart HTTP transport

```yaml
github:
  tokens:
    - token: "ghp_yourtoken"
      transport: "git"
      repo: "yourusername/tunnel-data"
```

> **Setup for `git` transport:** create a private GitHub repo (e.g. `yourusername/tunnel-data`) with at least one commit (add a README). Set the same `repo` on **both** client and server. Use a PAT with `repo` scope.

#### Example: Multiple accounts (increases capacity)

```yaml
github:
  tokens:
    - token: "ghp_account1_token"
      transport: "git"
      repo: "account1/tunnel-repo"
    - token: "ghp_account2_token"
      transport: "git"
      repo: "account2/tunnel-repo"
```

---

## Other Options

| Key | Default | Notes |
|---|---|---|
| `github.tokens` | — | **REQUIRED**. Array of objects, each with `token` (and optionally `transport`, `repo`). |
| `github.upstream_connections` | `2` | Number of pre-allocated upstream channels per token. |
| `github.batch_interval` | `100ms` | Client/server batch flush interval for multiplexed frames. |
| `github.fetch_interval` | `200ms` | Poll interval for receiving batches from the opposite side. |
| `github.api_timeout` | `10s` | HTTP timeout for REST API operations. |
| `socks.listen_addr` | `127.0.0.1:1080` | Client SOCKS5 address |
| `encryption.algorithm` | `xor` | `xor` (fast, insecure) or `aes` (AES-256-GCM) |
| `cleanup.enabled` | `false` | Enable server cleanup daemon (costs rate-limit quota for `gist` transport) |

### Client flags

| Flag | Default | Notes |
|---|---|---|
| `--no-tui` | false | Disable the TUI dashboard and print plain structured logs instead |
| `-config` | — | Path to the YAML config file |

## Building

```bash
make build-all   # both binaries
make test        # run tests
make test-race   # with race detector
```

## TUI Dashboard

The client starts in TUI mode by default. Pass `--no-tui` for plain structured logs.

```
gh-tunnel-client dev  uptime 2m14s  SOCKS5 127.0.0.1:1080
──────────────────────────────────────────────────────────────────────────────
Connections (2 active)  ↑ 18.4M  ↓ 3.2M total  [token0/gist:1  token1/git:1]
CONN-ID             VIA    DESTINATION             ↑ UP              ↓ DOWN
  conn_a1b2c3…    gist   1.184.1.34:443      1.2M (120K/s)     800K (80K/s)
  conn_d4e5f6…    git    api.ipify.org:443      17.2M (1.7M/s)    2.4M (240K/s)
──────────────────────────────────────────────────────────────────────────────
Token Quota
  ghp_****7EEM    gist  REST r/h   ███████████░░░░░░░░░░░░░  4412/5000  calls:588
                        WRITE/min  █░░░░░░░░░░░░░░░░░░░░░░░  74/80
                        WRITE/hr   ░░░░░░░░░░░░░░░░░░░░░░░░  494/500
  ghp_****7EEM    git   git  no REST quota · ~25 concurrent · bandwidth-throttled  calls:914
```

- **VIA column**: transport used per connection (`gist` or `git`)
- **↑/↓ total**: aggregate bytes transferred across all connections
- **calls:N**: cumulative API calls through that token since startup
- **3-bar quota (gist tokens)**: REST r/h, WRITE/min, WRITE/hr — three independent GitHub buckets; bars turn amber above 80% usage
- **git token row**: describes bandwidth limits instead of showing bars (no REST quota applies)

## Troubleshooting

| Problem | Likely cause |
|---|---|
| REST r/h drops fast (e.g. 5000 → 88 in 2 min) | Each poll of a `gist` connection burns one REST call; more active connections = faster burn. Switch to `git` transport. |
| Quota bar shows `0/100` / backoff timer | New account (100 req/hr limit). Switch to `git` transport or add tokens from different accounts. |
| `403 Forbidden` | Token missing `gist` scope (gist transport) or `repo` scope (git transport) |
| Connection hangs forever | Server not running, or different token/transport on one side |
| Decryption errors | `encryption.algorithm` mismatch between client and server |
| `transport=git requires repo` | Set `token.repo: "owner/repo"` in config |
| `git transport: clone … failed` | Repo does not exist or has no commits; create it with a README first |
| Old entries/files piling up | Lower `cleanup.dead_connection_ttl` on server |

## Uninstall

```bash
sudo bash uninstall.sh   # Linux with systemd
```

## License

MIT — no warranty. Authors not responsible for misuse.

