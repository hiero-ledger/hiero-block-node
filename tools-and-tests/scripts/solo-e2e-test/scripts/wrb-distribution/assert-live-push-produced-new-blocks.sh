#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 9, live half) —
# assert-live-push-produced-new-blocks.
#
# Runs between start-live-push.sh and stop-live-push.sh. The historical half of
# step 9 is proven separately by assert-bn1-historical-backfill-landed.sh right
# after bulk-load-historical-to-bn1.sh; this script only proves the "continue
# to move over" half: that the live-push loop (blocks push, wrappedBlocks ->
# BN1) kept completing iterations after the historical backfill, mirroring
# assert-live-wrap-produced-new-blocks.sh's own liveness check for the wrap
# loop.
#
# Reads:
#   (none directly — everything comes from /tmp/wrb-dist-push.state / .pid / .log
#   written by start-live-push.sh)
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   ASSERT_WAIT_TIMEOUT (default 100s — must fit inside YAML event timeout)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"

STATE_FILE="/tmp/wrb-dist-push.state"
PID_FILE="/tmp/wrb-dist-push.pid"
LOG_FILE="/tmp/wrb-dist-push.log"
BN1_GRPC_PORT="${BN1_GRPC_PORT:-40840}"

log() { echo "[wrb-dist-push-assert] $*"; }
fail() { echo "[wrb-dist-push-assert] ERROR: $*" >&2; exit 1; }

[[ -f "${STATE_FILE}" ]] || fail "State file ${STATE_FILE} not found; did start-live-push.sh run?"
[[ -f "${PID_FILE}" ]] || fail "PID file ${PID_FILE} not found; did start-live-push.sh run?"

# shellcheck disable=SC1090
source "${STATE_FILE}"
: "${initial_push_ok_count:?state file did not set initial_push_ok_count}"

worker_pid=$(cat "${PID_FILE}")
if kill -0 "${worker_pid}" 2>/dev/null; then
    log "Live-push worker still alive (pid ${worker_pid})"
else
    log "WARNING: worker pid ${worker_pid} is not alive; recent log:"
    tail -30 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
    fail "Live-push worker died before assertion"
fi

# ── Port-forward helpers ──────────────────────────────────────────────────────
# seed-bn-rsa-address-book.sh (delay 640) restarts BN1/BN2/BN3 sequentially.
# The test-runner's type:port-forward event at delay 670 fires while BN1 is
# still starting — solo-port-forward.sh finds no ready endpoint for BN1 and
# the port-forward fails silently.  Once BN1's new pod becomes Ready, there is
# no port-forward at :40840, so every push attempt gets "connection refused"
# and no "push OK" is ever logged.  Refresh the port-forward here (and again
# on each wait iteration) so the push worker can reach BN1 as soon as the pod
# is ready, regardless of cluster speed.
pf_log_dir="${TMPDIR:-/tmp}/wrb-dist-add-bn-pf"
mkdir -p "${pf_log_dir}"
setsid_prefix=""
command -v setsid >/dev/null 2>&1 && setsid_prefix="setsid"

refresh_bn1_port_forward() {
    local svc="block-node-1" local_port="${BN1_GRPC_PORT}"
    pkill -f "port-forward svc/${svc}.*${local_port}:" 2>/dev/null || true
    sleep 1
    local pf_log="${pf_log_dir}/${svc}-${local_port}.log"
    nohup ${setsid_prefix} kubectl --context "${CLUSTER_REFERENCE}" \
        --namespace "${NAMESPACE}" \
        port-forward "svc/${svc}" "${local_port}:40840" \
        >"${pf_log}" 2>&1 </dev/null &
    local deadline=$(( $(date +%s) + 15 ))
    until grep -q "Forwarding from" "${pf_log}" 2>/dev/null; do
        if (( $(date +%s) >= deadline )); then
            log "  port-forward for ${svc} (localhost:${local_port}) not yet up — pod may still be starting, will retry"
            return
        fi
        sleep 1
    done
    log "  port-forward for ${svc} ready on localhost:${local_port}."
}

log "Refreshing BN1 port-forward before polling (pod may still be starting after RSA seed restart)..."
refresh_bn1_port_forward

# At least one new successful push iteration since start-live-push.sh's snapshot.
# The live-push worker polls every LIVE_PUSH_POLL_SECONDS (default 30s). BN1 was
# restarted by seed-bn-rsa-address-book.sh and may only become Ready partway
# through this assertion window.  Poll for up to WAIT_TIMEOUT seconds so:
#   1. The port-forward refresh above (and each loop refresh below) keeps :40840
#      live as soon as the BN1 pod is Ready.
#   2. The push worker's next cycle has time to complete a successful push.
# YAML event timeout is 120s; keep WAIT_TIMEOUT safely below that.
WAIT_TIMEOUT="${ASSERT_WAIT_TIMEOUT:-100}"
waited=0
while true; do
    # No `|| echo 0`: grep -c prints 0 and exits 1 on no-match, so the fallback appends a
    # second line and the arithmetic below dies with `[[: 0\n0: syntax error`.
    current_push_ok_count=$( grep -cE '\] push OK' "${LOG_FILE}" 2>/dev/null )
    current_push_ok_count="${current_push_ok_count:-0}"
    log "push_ok_iterations: initial=${initial_push_ok_count} current=${current_push_ok_count} (waited ${waited}s)"
    if (( current_push_ok_count > initial_push_ok_count )); then
        log "OK: live-push completed $(( current_push_ok_count - initial_push_ok_count )) new iteration(s) during the observation window"
        exit 0
    fi
    if (( waited >= WAIT_TIMEOUT )); then
        break
    fi
    # Refresh the port-forward on each iteration: if BN1 just became Ready, this
    # establishes :40840 so the push worker can succeed on its next poll cycle.
    refresh_bn1_port_forward
    sleep 5
    waited=$(( waited + 5 ))
done

log "Live-push loop did not complete a new successful iteration. Recent worker log:"
tail -80 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
fail "No new push iterations between start and assertion (initial=${initial_push_ok_count} current=${current_push_ok_count})"
