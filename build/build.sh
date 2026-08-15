#!/usr/bin/env bash
# ==============================================================================
# MailBaby Compilation & Build Script (Linux / macOS)
# ==============================================================================

set -euo pipefail

# Project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Output binary location
OUTPUT_DIR="${ROOT_DIR}/build/bin"
BINARY_NAME="mailbaby"
OUTPUT_BIN="${OUTPUT_DIR}/${BINARY_NAME}"

# Build metadata
VERSION="${VERSION:-1.0.0}"
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo "dev")}"
BUILD_DATE="${BUILD_DATE:-$(date -u +"%Y-%m-%dT%H:%M:%SZ")}"

echo "=================================================="
echo " Building MailBaby"
echo "=================================================="
echo " Version    : ${VERSION}"
echo " Commit     : ${COMMIT}"
echo " Build Date : ${BUILD_DATE}"
echo " Output     : ${OUTPUT_BIN}"
echo "=================================================="

mkdir -p "${OUTPUT_DIR}"

cd "${ROOT_DIR}"

CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w \
      -X mailbaby/internal/cmd.Version=${VERSION} \
      -X mailbaby/internal/cmd.Commit=${COMMIT} \
      -X mailbaby/internal/cmd.BuildDate=${BUILD_DATE}" \
    -o "${OUTPUT_BIN}" \
    .

echo "[SUCCESS] Binary compiled successfully: ${OUTPUT_BIN}"
echo "Usage: ${OUTPUT_BIN} server -c config.yaml"
