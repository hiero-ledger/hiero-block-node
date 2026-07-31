#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6) — dump a Block Node's recent
# block-node-server container logs directly into the test runner's own
# stdout, which shows up live in CI's "Run Test Definitions" step log.
#
# Why this exists: the workflow's end-of-job "Collect BN and MN Logs" step
# (kubectl logs -l ... --since=24h --tail=-1, run once at the very end)
# has proven unreliable for catching a BN's mid-test activity — across
# multiple CI runs its captured log consistently stopped within ~2-3
# minutes of that BN's most recent restart, well before the backfill
# activity (or lack thereof) this script exists to observe. Dumping logs
# live, at a specific point in the test timeline via a "command" event,
# sidesteps that entirely — it's captured in the same log stream as
# everything else "Run Test Definitions" already prints.
#
# This is diagnostic-only: never fails the test (a kubectl error here just
# logs a note and moves on to the next target).
#
# Usage:
#     dump-bn-logs.sh <bn-index> [<bn-index> ...]
#     dump-bn-logs.sh 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   LOG_TAIL_LINES     (default 300 — lines of block-node-server container
#                       log to print per target)

set -uo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${LOG_TAIL_LINES:=300}"

[[ $# -ge 1 ]] || { echo "dump-bn-logs.sh: at least one BN index required (e.g. 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-dump-bn-logs] $*"; }

for bn_index in "$@"; do
    bn_name="block-node-${bn_index}"
    pod="${bn_name}-0"

    log "===== ${bn_name} (${pod}): last ${LOG_TAIL_LINES} lines of block-node-server ====="
    if ! kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        logs "${pod}" -c block-node-server --tail="${LOG_TAIL_LINES}" --timestamps 2>&1 \
        | sed "s/^/[${bn_name}] /"; then
        log "  (could not fetch logs for ${pod} — see kubectl error above, if any)"
    fi
    log "===== end ${bn_name } ====="

    log "===== ${bn_name} (${pod}): pod status/restart count ====="
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get pod "${pod}" -o jsonpath='{.status.containerStatuses[?(@.name=="block-node-server")]}' 2>&1 \
        | sed "s/^/[${bn_name}] /" || log "  (could not fetch pod status for ${pod})"
    echo ""
done

log "Done (diagnostic-only; never fails the test)."
exit 0
