# VPN Over GitHub

> **For educational / authorized research use only.**
> Likely violates [GitHub's ToS](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service). GitHub can read all gist/repo content. The default XOR cipher is not cryptographically secure.

A SOCKS5 tunnel that ships TCP packets through a private GitHub repo (`git`
transport) or private gists (`gist` transport). Useful where GitHub is
reachable but the rest of the internet isn't.

```
your app → SOCKS5 (client) → GitHub → server → internet
```

Two transports, picked per-token:

| Transport | Storage | PAT scope | Rate limit |
|---|---|---|---|
| **git** (recommended) | files in a private repo via git push/pull | `repo` | no REST quota; bandwidth-throttled |
| **gist** | content in a private gist via REST API | `gist` | 500 writes/hr per account |

## Does it actually work?

**Too slow. Not usable.** It's a proof-of-concept, not a real VPN. One tunnel round-trip
is one git push + one fetch ≈ **1.5–2 s** even on a fast connection.

The tool exists because it's an interesting hack, not because GitHub is a
good carrier for IP traffic.

## Install

### Server (Linux, systemd)

```bash
sudo bash -c "$(curl -Ls https://raw.githubusercontent.com/sartoopjj/vpn-over-github/main/install.sh)"
```

The installer drops a binary at `/opt/gh-tunnel/gh-tunnel-server`, writes
`/opt/gh-tunnel/server-config.yaml`, and starts a systemd service.

### Client

Download the latest release binary from the GitHub releases page and run:

```bash
./gh-tunnel-client -config client_config.yaml          # TUI dashboard
./gh-tunnel-client --no-tui -config client_config.yaml # plain logs
```

Then point any SOCKS5-aware app at `127.0.0.1:1080`:

```bash
curl -x socks5h://127.0.0.1:1080 https://api.ipify.org
```

### Build from source

```bash
make build-all    # both binaries → ./build/
make test         # unit tests
make test-race    # with race detector
```

Requires Go 1.21+.

## Configure

Both client and server take a YAML file. Tokens are an array; each entry can
override `transport`, `repo`, `batch_interval`, `fetch_interval`, and
`upstream_connections`. Example:

```yaml
github:
  tokens:
    - token: "ghp_yourtoken"
      transport: "git"
      repo: "yourname/your-private-tunnel-repo"
      batch_interval: 1500ms
      fetch_interval: 1500ms
    # gist alternative
    # - token: "ghp_yourtoken"
    #   transport: "gist"
    #   batch_interval: 1500ms
    #   fetch_interval: 1500ms
  batch_interval: 1500ms         # global defaults (used when token omits)
  fetch_interval: 1500ms
  api_timeout: 10s

socks:
  listen_addr: "127.0.0.1:1080"
  timeout: 30s
  buffer_size: 1048576           # 1 MiB — bigger Reads from the SOCKS app

encryption:
  algorithm: "xor"               # or "aes" (AES-256-GCM)
```

Server-side, `proxy.buffer_size` (default 2 MiB) controls per-Read size from
upstream destinations — bigger means larger frames, useful for video.

For the `git` transport, create a private repo with at least one commit
(e.g. an initial `README`). Use the same `repo` and the same encryption
algorithm on both sides.

`example_client_config.yaml` and `example_server_config.yaml` are starting
points.

## Uninstall

```bash
sudo bash uninstall.sh
```

## License

MIT — no warranty.
