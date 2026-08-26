#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6) — diagnose-bn-rsa-verification.
#
# Diagnostic-only script that captures the exact RSA verification failure
# reason from BN2's (and optionally other BNs') logs after the seed restart.
# This script never returns a non-zero exit code — it is purely informational.
#
# The ExtendedMerkleTreeSession WARNING message tells us which failure path hit:
#   "No address book era covers block N"     → rsaKeyByNodeId is empty
#                                              (seed file not loaded/parsed)
#   "has no record-file signatures"          → wrapped blocks have empty signature
#                                              list (wrap-time address book extraction
#                                              failed → WRB used mainnet genesis keys
#                                              → Solo signatures rejected at wrap time
#                                              → empty proof.recordFileSignatures())
#   "RSA signature from node N failed"       → key loaded but crypto verification fails
#                                              (wrong key in seed)
#   "RSA WRB proof rejected for block N: 0" → key not in rsaKeyByNodeId (node-id mismatch
#                                              between sig.nodeId() and seeded nodeId)
#
# Usage:
#   diagnose-bn-rsa-verification.sh [<bn-index>...]
#   Default: 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   ENV_FILE           (default /tmp/wrb-distribution-step12.env)

set -uo pipefail

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"

log() { echo "[wrb-dist-diagnose-rsa] $*"; }

target_indices=("${@:-2 3}")
if [[ $# -eq 0 ]]; then
    target_indices=(2 3)
fi

rsa_file="/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json"

for target_index in "${target_indices[@]}"; do
    target_pod="block-node-${target_index}-0"
    log "======================================================="
    log "Diagnosing BN${target_index} (pod: ${target_pod})"
    log "======================================================="

    log "--- Seeded rsa-bootstrap-roster.json ---"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        exec "${target_pod}" -c block-node-server -- \
        cat "${rsa_file}" 2>/dev/null \
        || log "WARNING: ${rsa_file} not found on ${target_pod}"

    log ""
    log "--- ExtendedMerkleTreeSession WARNING log lines (last 600s) ---"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        logs "pod/${target_pod}" -c block-node-server --since=600s 2>/dev/null \
        | grep -iE \
            "No address book era|record-file signatures|RSA signature from node|RSA WRB proof rejected|RSA WRB proof for block|MISSING_VERIFICATION|address book era covers|verifyRsaProof|ExtendedMerkle|BAD_BLOCK_PROOF|Unsupported SignedRecordFileProof" \
        | head -50 \
        || log "No matching WARNING log entries"
    log ""
done

log "Diagnosis complete."
