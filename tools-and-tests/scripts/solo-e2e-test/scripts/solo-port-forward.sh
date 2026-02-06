#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Discovers and forwards ports for some deployed Solo services of interest.
# Supports multiple block nodes, mirror nodes, relay nodes, and explorer nodes.
#
# Usage:
#   ./solo-port-forward.sh --namespace <namespace>
#
# Options:
#   --namespace NAMESPACE    Kubernetes namespace (required)
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

function show_help {
  cat << 'EOF'
Usage: solo-port-forward.sh --namespace <namespace>

Discovers and forwards ports for some deployed Solo services of interest.

Options:
  --namespace NAMESPACE    Kubernetes namespace (required)
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

echo "Discovering deployed services in namespace: ${NAMESPACE}"

# Kill existing port-forwards
pkill -f "kubectl port-forward" 2>/dev/null || true

# Arrays to collect endpoints for summary
declare -a CN_ENDPOINTS=()
declare -a BN_ENDPOINTS=()
declare -a BN_METRICS_ENDPOINTS=()
declare -a MN_ENDPOINTS=()
declare -a RELAY_ENDPOINTS=()
declare -a EXPLORER_ENDPOINTS=()
declare -a METRICS_ENDPOINTS=()

echo "Setting up port forwards..."

# Consensus node (single)
if kubectl get svc haproxy-node1-svc -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl port-forward svc/haproxy-node1-svc -n "${NAMESPACE}" 50211:50211 >/dev/null 2>&1 &
  CN_ENDPOINTS+=("localhost:50211")
fi

# Block nodes (40840, 40841, ...) - match only "block-node-N" not monitoring services
BN_PORT=40840
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "block-node-[0-9]+$" | sort); do
  svc_name=$(basename "$svc")
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" ${BN_PORT}:40840 >/dev/null 2>&1 &
  BN_ENDPOINTS+=("localhost:${BN_PORT} (${svc_name})")
  BN_PORT=$((BN_PORT + 1))
done

# Block node metrics (16007, 16008, ...) - match only "block-node-N" not monitoring services
BN_METRICS_PORT=16007
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "block-node-[0-9]+$" | sort); do
  svc_name=$(basename "$svc")
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" ${BN_METRICS_PORT}:16007 >/dev/null 2>&1 &
  BN_METRICS_ENDPOINTS+=("http://localhost:${BN_METRICS_PORT}/metrics (${svc_name})")
  BN_METRICS_PORT=$((BN_METRICS_PORT + 1))
done

# Mirror REST nodes (5551, 5552, ...)
MN_PORT=5551
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "mirror-.*-rest$" | sort); do
  svc_name=$(basename "$svc")
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" ${MN_PORT}:80 >/dev/null 2>&1 &
  MN_ENDPOINTS+=("http://localhost:${MN_PORT} (${svc_name})")
  MN_PORT=$((MN_PORT + 1))
done

# Relay nodes (JSON-RPC: 7546+, WebSocket: 8546+)
RELAY_PORT=7546
RELAY_WS_PORT=8546
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "relay-[0-9]+" | grep -v "\-ws" | sort); do
  svc_name=$(basename "$svc")
  ws_svc="${svc_name}-ws"
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" ${RELAY_PORT}:7546 >/dev/null 2>&1 &
  RELAY_ENDPOINTS+=("http://localhost:${RELAY_PORT} JSON-RPC (${svc_name})")
  if kubectl get svc "${ws_svc}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    kubectl port-forward "svc/${ws_svc}" -n "${NAMESPACE}" ${RELAY_WS_PORT}:8546 >/dev/null 2>&1 &
    RELAY_ENDPOINTS+=("ws://localhost:${RELAY_WS_PORT} WebSocket (${ws_svc})")
  fi
  RELAY_PORT=$((RELAY_PORT + 1))
  RELAY_WS_PORT=$((RELAY_WS_PORT + 1))
done

# Explorer nodes (8080, 8081, ...)
EXPLORER_PORT=8080
for svc in $(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "explorer" | sort); do
  svc_name=$(basename "$svc")
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" ${EXPLORER_PORT}:80 >/dev/null 2>&1 &
  EXPLORER_ENDPOINTS+=("http://localhost:${EXPLORER_PORT} (${svc_name})")
  EXPLORER_PORT=$((EXPLORER_PORT + 1))
done

# Grafana (if local metrics enabled)
GRAFANA_SVC=$(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep grafana | head -1)
if [[ -n "$GRAFANA_SVC" ]]; then
  svc_name=$(basename "$GRAFANA_SVC")
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" 3000:80 >/dev/null 2>&1 &
  METRICS_ENDPOINTS+=("http://localhost:3000 Grafana (admin/admin)")
fi

# Prometheus (if local metrics enabled)
PROM_SVC=$(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "kubepromstack-prometheus" | head -1)
if [[ -n "$PROM_SVC" ]]; then
  svc_name=$(basename "$PROM_SVC")
  kubectl port-forward "svc/${svc_name}" -n "${NAMESPACE}" 9090:9090 >/dev/null 2>&1 &
  METRICS_ENDPOINTS+=("http://localhost:9090 Prometheus")
fi

sleep 2

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
