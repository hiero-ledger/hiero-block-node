#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Shared helpers for the backfill-with-live-tail E2E scripts (#3053).
# Sourced, never executed directly.
#
# Expects the sourcing script to set LOG_PREFIX before sourcing.

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"

log() { echo "[${LOG_PREFIX}] $*"; }
fail() { echo "[${LOG_PREFIX}] ERROR: $*" >&2; exit 1; }

# Local ports that solo-port-forward.sh binds a BN's endpoints to:
# metrics 16007 and gRPC 40840 for block-node-1, then one higher per index.
bn_metrics_port() { echo $((16006 + $1)); }
bn_grpc_port() { echo $((40839 + $1)); }

# Read one Prometheus metric from a BN over its port-forward and round it to an
# integer -- gauges are exported in floating-point form ("612.0"), which bash
# arithmetic rejects. Prints nothing and returns 1 when the metric is absent.
read_bn_metric_int() {
    local bn_index="$1" metric="$2" raw
    raw=$(curl -s --max-time 5 "http://localhost:$(bn_metrics_port "${bn_index}")/metrics" 2>/dev/null |
        awk -v m="${metric}" '$1 == m { print $2; exit }')
    [[ -n "${raw}" ]] || return 1
    printf "%.0f" "${raw}" 2>/dev/null || return 1
}

# Highest block number the given BN has seen on its inbound publisher stream.
# Used as the network's current height: it tracks the Consensus Node's chain
# head without needing grpcurl or the protobuf sources.
read_bn_height() {
    read_bn_metric_int "$1" "blocknode_publisher_highest_block_number_inbound"
}
