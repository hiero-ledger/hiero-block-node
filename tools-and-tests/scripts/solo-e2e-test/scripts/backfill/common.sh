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
# A single read is retried: a port-forward can drop mid-run, and its supervisor
# needs a moment to bring it back. Without this, one dropped connection aborts a
# 25-minute test from a one-shot caller.
read_bn_metric_int() {
    local bn_index="$1" metric="$2" body raw attempt max_attempts=3
    for ((attempt = 1; attempt <= max_attempts; attempt++)); do
        body=$(curl -s --max-time 5 "http://localhost:$(bn_metrics_port "${bn_index}")/metrics" 2>/dev/null)
        # Only an unreachable endpoint is worth retrying. One that answers without
        # the metric is the ordinary "not exported yet" case, which polling callers
        # handle themselves, so it returns at once instead of costing them 19s.
        if [[ -n "${body}" ]]; then
            raw=$(awk -v m="${metric}" '$1 == m { print $2; exit }' <<<"${body}")
            if [[ -z "${raw}" ]]; then
                return 1
            fi
            printf "%.0f" "${raw}" 2>/dev/null || return 1
            return 0
        fi
        if [[ "${attempt}" -lt "${max_attempts}" ]]; then
            sleep 2
        fi
    done
    return 1
}

# Highest block number the given BN has seen on its inbound publisher stream.
# Used as the network's current height: it tracks the Consensus Node's chain
# head without needing grpcurl or the protobuf sources.
read_bn_height() {
    read_bn_metric_int "$1" "blocknode_publisher_highest_block_number_inbound"
}
