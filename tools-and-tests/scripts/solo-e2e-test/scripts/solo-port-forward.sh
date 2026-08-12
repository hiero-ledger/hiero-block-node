#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Discovers and forwards ports for some deployed Solo services of interest.
# Supports multiple block nodes, mirror nodes, relay nodes, and explorer nodes.
#
# Usage:
#   ./solo-port-forward.sh --namespace <namespace>
#   ./solo-port-forward.sh --namespace <namespace> --stop
#
# Options:
#   --namespace NAMESPACE    Kubernetes namespace (required)
#   --stop                   Tear down this namespace's port forwards and exit
#   --help                   Show this help message
#
# Port Mappings:
#   Block Nodes:     40840, 40841, 40842, ...
#   BN Metrics:      16007, 16008, 16009, ...
#   Mirror REST:     5551, 5552, 5553, ...
#   Relay JSON-RPC:  7546, 7547, ...
#   Relay WebSocket: 8546, 8547, ...
#   Explorer:        8080, 8081, ...
#   Consensus:       50211 (single haproxy)
#   Grafana:         3000 (if local metrics enabled)
#   Prometheus:      9090 (if local metrics enabled)

set -o pipefail

NAMESPACE=""
STOP_ONLY="false"

function show_help {
  cat << 'EOF'
Usage: solo-port-forward.sh --namespace <namespace>
       solo-port-forward.sh --namespace <namespace> --stop

Discovers and forwards ports for some deployed Solo services of interest.

Options:
  --namespace NAMESPACE    Kubernetes namespace (required)
  --stop                   Tear down this namespace's port forwards and exit
  --help                   Show this help message

Port Mappings:
  Block Nodes:     40840, 40841, 40842, ...
  BN Metrics:      16007, 16008, 16009, ...
  Mirror REST:     5551, 5552, 5553, ...
  Relay JSON-RPC:  7546, 7547, ...
  Relay WebSocket: 8546, 8547, ...
  Explorer:        8080, 8081, ...
  Consensus:       50211 (single haproxy)
  Grafana:         3000 (if local metrics enabled)
  Prometheus:      9090 (if local metrics enabled)
EOF
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --stop)
      STOP_ONLY="true"
      shift
      ;;
    --help|-h)
      show_help
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

[[ -z "${NAMESPACE}" ]] && { echo "ERROR: --namespace is required"; exit 1; }

# Both the sentinel and the pkill pattern are namespace-scoped, so forwards a
# developer holds open against another namespace survive this script. The pattern
# is anchored on the "-n ${NAMESPACE} " that pf() below emits, so a namespace that
# happens to be a substring of a service name cannot match on the service instead.
# The long-lived forwards wrb-distribution's scripts spawn pass their namespace as
# "--namespace NS" before the verb, so they are left alone, as they always were.
: "${PF_KEEPALIVE_FILE:=/tmp/solo-port-forward-${NAMESPACE}.keepalive}"
PF_PATTERN="kubectl.*port-forward.*-n ${NAMESPACE} "

# Stop every forward and its supervisor. Remove the sentinel first so a
# supervisor whose kubectl is killed below exits instead of restarting it; the
# sleep outlasts the supervisor's own retry sleep, so every one of them has
# woken up and seen the sentinel gone by the time it returns. The second pkill
# reaps a forward started in the window between the rm and the first pkill,
# whose supervisor then exited without ever killing it.
stop_forwards() {
  rm -f "${PF_KEEPALIVE_FILE}"
  pkill -f "${PF_PATTERN}" 2>/dev/null || true
  sleep 3
  pkill -f "${PF_PATTERN}" 2>/dev/null || true
}

if [[ "${STOP_ONLY}" == "true" ]]; then
  echo "Stopping port forwards for namespace: ${NAMESPACE}"
  stop_forwards
  echo "Port forwards stopped."
  exit 0
fi

echo "Discovering deployed services in namespace: ${NAMESPACE}"

# Supervise a port-forward: kubectl exits whenever its connection drops, which
# silently leaves a test talking to a dead local port. Restart it until the
# keepalive sentinel is removed.
#   $1 = svc/<name>, $2 = <local>:<remote>
pf() {
  ( delay=2
    while [[ -f "${PF_KEEPALIVE_FILE}" ]]; do
      started=${SECONDS}
      kubectl port-forward "$1" -n "${NAMESPACE}" "$2" >/dev/null 2>&1
      # A forward that ran for a while dropped its connection: retry promptly. One that
      # exits instantly is a service that is gone, so back off up to 30s rather than
      # respawning every 2s for the rest of a 25-minute run.
      if (( SECONDS - started >= 10 )); then
        delay=2
      else
        delay=$(( delay * 2 > 30 ? 30 : delay * 2 ))
      fi
      # Slept in 2s slices so a removed sentinel is still noticed within the window
      # stop_forwards waits out, however long the backoff has grown.
      for (( slept = 0; slept < delay; slept += 2 )); do
        [[ -f "${PF_KEEPALIVE_FILE}" ]] || break
        sleep 2
      done
    done ) 2>/dev/null &
}

# Stop any previous generation of forwards and their supervisors before starting
# a new one, so they neither restart nor hold on to the local ports.
stop_forwards

# An interrupted run (Ctrl-C, CI cancel) would otherwise leave the sentinel and
# a half-built generation of forwards behind, restarting themselves forever.
# Not on EXIT: a successful run is *meant* to leave its forwards running.
trap 'stop_forwards' INT TERM

touch "${PF_KEEPALIVE_FILE}"

# Arrays to collect endpoints for summary
declare -a CN_ENDPOINTS=()
declare -a BN_ENDPOINTS=()
declare -a BN_METRICS_ENDPOINTS=()
declare -a MN_ENDPOINTS=()
declare -a RELAY_ENDPOINTS=()
declare -a EXPLORER_ENDPOINTS=()
declare -a METRICS_ENDPOINTS=()

# Collects the names of any forward that failed to bind, for a final visible
# warning -- each individual `kubectl port-forward &` used to be launched with
# its output sent to /dev/null and never checked, so a transient failure (e.g.
# a race right after the `pkill` above) was silently invisible: the process
# just wasn't there afterward, with no error anywhere.
declare -a FAILED_FORWARDS=()
PF_LOG_DIR="${TMPDIR:-/tmp}/solo-port-forward-logs"
mkdir -p "${PF_LOG_DIR}"

# Starts one `kubectl port-forward` in the background and waits for its own
# log output to confirm the tunnel actually bound, instead of firing-and-
# forgetting. Mirrors the wait_for_port_forward pattern already used in
# bulk-load-historical-to-bn1.sh / stage-tss-data-on-bn1.sh: poll the
# port-forward's own "Forwarding from" line rather than nc/bash's /dev/tcp,
# whose availability varies across environments this script runs in.
function start_port_forward {
  local svc_name="$1" local_port="$2" remote_port="$3"
  local log_file="${PF_LOG_DIR}/${svc_name}-${local_port}.log"
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" "${local_port}:${remote_port}" > "${log_file}" 2>&1 &
  for _ in $(seq 1 15); do
    grep -q "Forwarding from" "${log_file}" 2>/dev/null && return 0
    sleep 1
  done
  echo "WARNING: port-forward for ${svc_name} (localhost:${local_port}) never came up:" >&2
  sed 's/^/    /' "${log_file}" >&2 || true
  FAILED_FORWARDS+=("${svc_name} (localhost:${local_port})")
  return 1
}

echo "Setting up port forwards..."

# Consensus node (single)
if kubectl get svc haproxy-node1-svc -n "${NAMESPACE}" >/dev/null 2>&1; then
  pf "svc/haproxy-node1-svc" "50211:50211"
  CN_ENDPOINTS+=("localhost:50211")
fi

# Block nodes (40840, 40841, ...) - match only "block-node-N" not monitoring services
BN_PORT=40840
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "block-node-[0-9]+$" | sort); do
  svc_name=$(basename "$svc")
  pf "svc/${svc_name}" "${BN_PORT}:40840"
  BN_ENDPOINTS+=("localhost:${BN_PORT} (${svc_name})")
  BN_PORT=$((BN_PORT + 1))
done

# Block node metrics (16007, 16008, ...) - match only "block-node-N" not monitoring services
BN_METRICS_PORT=16007
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "block-node-[0-9]+$" | sort); do
  svc_name=$(basename "$svc")
  pf "svc/${svc_name}" "${BN_METRICS_PORT}:16007"
  BN_METRICS_ENDPOINTS+=("http://localhost:${BN_METRICS_PORT}/metrics (${svc_name})")
  BN_METRICS_PORT=$((BN_METRICS_PORT + 1))
done

# Mirror REST nodes (5551, 5552, ...)
MN_PORT=5551
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "mirror-.*-rest$" | sort); do
  svc_name=$(basename "$svc")
  pf "svc/${svc_name}" "${MN_PORT}:80"
  MN_ENDPOINTS+=("http://localhost:${MN_PORT} (${svc_name})")
  MN_PORT=$((MN_PORT + 1))
done

# Relay nodes (JSON-RPC: 7546+, WebSocket: 8546+)
RELAY_PORT=7546
RELAY_WS_PORT=8546
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "relay-[0-9]+" | grep -v "\-ws" | sort); do
  svc_name=$(basename "$svc")
  ws_svc="${svc_name}-ws"
  pf "svc/${svc_name}" "${RELAY_PORT}:7546"
  RELAY_ENDPOINTS+=("http://localhost:${RELAY_PORT} JSON-RPC (${svc_name})")
  if kubectl get svc "${ws_svc}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    pf "svc/${ws_svc}" "${RELAY_WS_PORT}:8546"
    RELAY_ENDPOINTS+=("ws://localhost:${RELAY_WS_PORT} WebSocket (${ws_svc})")
  fi
  RELAY_PORT=$((RELAY_PORT + 1))
  RELAY_WS_PORT=$((RELAY_WS_PORT + 1))
done

# Explorer nodes (8080, 8081, ...)
EXPLORER_PORT=8080
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "explorer" | sort); do
  svc_name=$(basename "$svc")
  pf "svc/${svc_name}" "${EXPLORER_PORT}:80"
  EXPLORER_ENDPOINTS+=("http://localhost:${EXPLORER_PORT} (${svc_name})")
  EXPLORER_PORT=$((EXPLORER_PORT + 1))
done

# Grafana (if local metrics enabled)
GRAFANA_SVC=$(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep grafana | head -1)
if [[ -n "$GRAFANA_SVC" ]]; then
  svc_name=$(basename "$GRAFANA_SVC")
  pf "svc/${svc_name}" "3000:80"
  METRICS_ENDPOINTS+=("http://localhost:3000 Grafana (admin/admin)")
fi

# Prometheus (if local metrics enabled)
PROM_SVC=$(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "kubepromstack-prometheus" | head -1)
if [[ -n "$PROM_SVC" ]]; then
  svc_name=$(basename "$PROM_SVC")
  pf "svc/${svc_name}" "9090:9090"
  METRICS_ENDPOINTS+=("http://localhost:9090 Prometheus")
fi

# Print formatted summary
echo ""
echo "Port Forwards Active"
echo "===================="

if [[ ${#CN_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Consensus Nodes:"
  for ep in "${CN_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

if [[ ${#BN_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Block Nodes (gRPC):"
  for ep in "${BN_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

if [[ ${#BN_METRICS_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Block Node Metrics:"
  for ep in "${BN_METRICS_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

if [[ ${#MN_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Mirror Nodes (REST):"
  for ep in "${MN_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

if [[ ${#RELAY_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Relay Nodes:"
  for ep in "${RELAY_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

if [[ ${#EXPLORER_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Explorer:"
  for ep in "${EXPLORER_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

if [[ ${#METRICS_ENDPOINTS[@]} -gt 0 ]]; then
  echo ""
  echo "Metrics Dashboards:"
  for ep in "${METRICS_ENDPOINTS[@]}"; do echo "  $ep"; done
fi

echo ""

if [[ ${#FAILED_FORWARDS[@]} -gt 0 ]]; then
  echo "WARNING: ${#FAILED_FORWARDS[@]} port-forward(s) never came up (see warnings above):"
  for ep in "${FAILED_FORWARDS[@]}"; do echo "  $ep"; done
  echo ""
fi
