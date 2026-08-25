#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Backfill-with-live-tail E2E (#3053) -- block until the network has produced at
# least MIN_HEIGHT blocks.
#
# The behaviour this test asserts (historical backfill closing a gap while the
# publisher keeps accepting live blocks) only exists while there is a historical
# gap big enough to outlive the observation window. The wiped Block Node
# therefore needs a few hundred blocks of history to recover, so the test waits
# for the network to reach a floor before simulating the data loss. If the
# network is already past the floor this returns immediately and costs nothing.
#
# Usage:
#     wait-for-network-height.sh [reference-bn-index]
#     wait-for-network-height.sh 2
#
# Reads:
#   MIN_HEIGHT     (default 600)  height the network must reach
#   POLL_INTERVAL  (default 15)   seconds between polls
#   HEIGHT_TIMEOUT   (default 2400) seconds before giving up

set -euo pipefail

LOG_PREFIX="backfill-live-tail-height"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source-path=SCRIPTDIR source=common.sh
source "${SCRIPT_DIR}/common.sh"

: "${MIN_HEIGHT:=600}"
: "${POLL_INTERVAL:=15}"
: "${HEIGHT_TIMEOUT:=2400}"

reference_index="${1:-2}"

log "Waiting for block-node-${reference_index} to report height >= ${MIN_HEIGHT} (timeout ${HEIGHT_TIMEOUT}s)..."

deadline=$(($(date +%s) + HEIGHT_TIMEOUT))
height=""
while true; do
    height=$(read_bn_height "${reference_index}") || height=""
    if [[ -n "${height}" ]] && ((height >= MIN_HEIGHT)); then
        log "Network reached height ${height} (floor ${MIN_HEIGHT})."
        exit 0
    fi
    (($(date +%s) < deadline)) || break
    log "  height=${height:-unavailable}, waiting ${POLL_INTERVAL}s..."
    sleep "${POLL_INTERVAL}"
done

fail "network did not reach height ${MIN_HEIGHT} within ${HEIGHT_TIMEOUT}s (last height=${height:-unavailable})"
