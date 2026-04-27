#!/usr/bin/env bash
# install.sh — Install gh-tunnel-server on Linux with systemd
# Usage: curl -fsSL https://raw.githubusercontent.com/sartoopjj/vpn-over-github/main/install.sh | bash
#
# ============================================================
# ⚠️  WARNING: This tool tunnels traffic through GitHub.
#    Doing so likely violates GitHub's Terms of Service.
#    Use ONLY for authorized security research, personal testing,
#    or bypassing censorship where legally permitted.
#    You are solely responsible for all consequences of use.
# ============================================================

set -euo pipefail

# ---- Configuration ----
REPO="sartoopjj/vpn-over-github"
INSTALL_DIR="/opt/gh-tunnel"
SERVICE_NAME="gh-tunnel-server"
CONFIG_FILE="${INSTALL_DIR}/server-config.yaml"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
BINARY="${INSTALL_DIR}/gh-tunnel-server"
VERSION_FILE="${INSTALL_DIR}/VERSION"

# ---- Colors ----
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m'

log()  { echo -e "${GREEN}[install]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

# ---- Detect OS and architecture ----
detect_platform() {
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    case "$(uname -m)" in
        x86_64)  arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        armv7*)  arch="arm_v7" ;;
        *) err "Unsupported architecture: $(uname -m)" ;;
    esac
    case "$os" in
        linux)  ;;
        darwin) warn "macOS detected. systemd installation is Linux-only." ;;
        *)      err "Unsupported OS: $os" ;;
    esac
    echo "${os}_${arch}"
}

# ---- Fetch latest release version ----
get_latest_version() {
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"v\([^"]*\)".*/\1/'
}

# ---- Download binary ----
download_binary() {
    local version="$1"
    local platform="$2"
    local filename="gh-tunnel-server_${platform}"
    local url="https://github.com/${REPO}/releases/download/v${version}/${filename}"

    log "Downloading ${filename} from GitHub release ${version} ..."
    mkdir -p "${INSTALL_DIR}"
    curl -fsSL -o "${BINARY}.tmp" "${url}"
    chmod +x "${BINARY}.tmp"

    # Verify checksum
    local checksums_url="https://github.com/${REPO}/releases/download/v${version}/sha256sums.txt"
    local checksums
    checksums="$(curl -fsSL "${checksums_url}")"
    local expected
    expected="$(echo "${checksums}" | grep "${filename}" | awk '{print $1}')"
    if [ -n "${expected}" ]; then
        local actual
        actual="$(sha256sum "${BINARY}.tmp" | awk '{print $1}')"
        if [ "${actual}" != "${expected}" ]; then
            rm -f "${BINARY}.tmp"
            err "Checksum mismatch! Expected ${expected}, got ${actual}"
        fi
        log "Checksum verified: ${actual}"
    else
        warn "No checksum found for ${filename}; skipping verification"
    fi

    mv "${BINARY}.tmp" "${BINARY}"
    echo "${version}" > "${VERSION_FILE}"
    log "Binary installed to ${BINARY}"
}

# ---- Interactive configuration ----
configure() {
    if [ -f "${CONFIG_FILE}" ]; then
        warn "Config file already exists at ${CONFIG_FILE}"
        read -r -p "Overwrite? [y/N] " confirm
        if [ "${confirm}" != "y" ] && [ "${confirm}" != "Y" ]; then
            log "Keeping existing config."
            return
        fi
    fi

    echo ""
    log "GitHub Tunnel Server Configuration"
    echo "You need a GitHub Personal Access Token (PAT)."
    echo "  gist transport  → PAT with 'gist' scope"
    echo "  git  transport  → PAT with 'repo' scope + a private repo"
    echo "Create one at: https://github.com/settings/tokens"
    echo ""

    read -r -p "Enter GitHub token (ghp_... or github_pat_...): " TOKEN
    if [ -z "${TOKEN}" ]; then
        err "Token is required."
    fi

    read -r -p "Transport [gist/git] (default: git): " TRANSPORT
    TRANSPORT="${TRANSPORT:-git}"

    REPO_LINE=""
    if [ "${TRANSPORT}" = "git" ]; then
        read -r -p "GitHub repo for git transport (owner/repo): " GIT_REPO
        if [ -z "${GIT_REPO}" ]; then
            err "Repo is required for git transport."
        fi
        REPO_LINE="\n      repo: \"${GIT_REPO}\""
    fi

    read -r -p "Encryption algorithm [xor/aes] (default: xor): " ALGO
    ALGO="${ALGO:-xor}"

    # Per-transport defaults that respect GitHub rate limits:
    #   git : no REST quota; 500 ms keeps each push completing in time.
    #   gist: 500 writes/hr/account secondary cap → batch ≥ 8 s.
    if [ "${TRANSPORT}" = "gist" ]; then
        DEFAULT_BATCH_MS=8000
        DEFAULT_FETCH_MS=1000
    else
        DEFAULT_BATCH_MS=500
        DEFAULT_FETCH_MS=1000
    fi

    read -r -p "Batch interval in ms [default: ${DEFAULT_BATCH_MS}]: " BATCH_MS
    BATCH_MS="${BATCH_MS:-${DEFAULT_BATCH_MS}}"

    read -r -p "Fetch interval in ms [default: ${DEFAULT_FETCH_MS}]: " FETCH_MS
    FETCH_MS="${FETCH_MS:-${DEFAULT_FETCH_MS}}"

    # shellcheck disable=SC2059
    printf "github:\n  tokens:\n    - token: \"%s\"\n      transport: \"%s\"%b\n      batch_interval: %sms\n      fetch_interval: %sms\n  upstream_connections: 1\n  batch_interval: %sms\n  fetch_interval: %sms\n  api_timeout: 10s\n\ncleanup:\n  enabled: true\n  interval: 10m\n  dead_connection_ttl: 15m\n\nproxy:\n  target_timeout: 30s\n  buffer_size: 2097152\n\nencryption:\n  algorithm: %s\n\nlogging:\n  level: info\n  format: text\n" \
        "${TOKEN}" "${TRANSPORT}" "${REPO_LINE}" "${BATCH_MS}" "${FETCH_MS}" "${BATCH_MS}" "${FETCH_MS}" "${ALGO}" > "${CONFIG_FILE}"
    chmod 600 "${CONFIG_FILE}"
    log "Config written to ${CONFIG_FILE} (permissions: 600)"
}

# ---- Install systemd service ----
install_service() {
    if [ "$(id -u)" -ne 0 ]; then
        warn "Not running as root. Skipping systemd service installation."
        warn "Run with sudo to install as a system service."
        return
    fi

    chown -R nobody "${INSTALL_DIR}" 2>/dev/null || true

    cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=GitHub Tunnel Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${BINARY} -config ${CONFIG_FILE}
Restart=on-failure
RestartSec=5
User=nobody
Group=nogroup
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=${INSTALL_DIR}
StandardOutput=journal
StandardError=journal
SyslogIdentifier=gh-tunnel-server

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}"
    systemctl start "${SERVICE_NAME}"
    log "Service ${SERVICE_NAME} installed and started"
    log "View logs: journalctl -u ${SERVICE_NAME} -f"
}

# ---- Main ----
main() {
    echo ""
    echo "======================================================"
    echo "  ⚠️  vpn-over-github — GitHub Tunnel Server"
    echo "======================================================"
    echo "  WARNING: This tool may violate GitHub's Terms of"
    echo "  Service. Use only with explicit authorization."
    echo "======================================================"
    echo ""
    read -r -p "Do you understand and accept the risks? [yes/no] " accept
    if [ "${accept}" != "yes" ]; then
        echo "Installation aborted."
        exit 0
    fi

    local platform version
    platform="$(detect_platform)"
    version="$(get_latest_version)"
    log "Latest version: ${version}, platform: ${platform}"

    download_binary "${version}" "${platform}"
    configure
    install_service

    echo ""
    log "Installation complete!"
    log "Binary: ${BINARY}"
    log "Config: ${CONFIG_FILE}"
    if [ "$(id -u)" -eq 0 ]; then
        log "Service: systemctl status ${SERVICE_NAME}"
    else
        log "Run manually: ${BINARY} -config ${CONFIG_FILE}"
    fi
}

main "$@"
