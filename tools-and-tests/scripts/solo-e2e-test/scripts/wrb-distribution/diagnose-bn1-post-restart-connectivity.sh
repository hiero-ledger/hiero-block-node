#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6) — standalone diagnostic for a
# post-BN1-restart connectivity failure seen in CI: after
# stage-tss-data-on-bn1.sh restarts BN1's pod, BN2/BN3's roster-bootstrap-tss
# plugin fails EVERY subsequent poll of BN1 with
# "java.net.SocketException: Socket closed", even though the exact same
# address:port (block-node-1.solo-network.svc.cluster.local:<port>) is what
# BackfillFetcher used successfully from BN2 *before* the restart, and what
# the external test runner's `kubectl port-forward svc/block-node-1 ...`
# (which resolves the Service to a pod IP once, client-side, and bypasses
# kube-proxy/Endpoints routing entirely) still reaches successfully *after*
# the restart.
#
# This script isolates whether the failure is:
#   (a) Service/Endpoints staleness — the block-node-1 Service's Endpoints
#       object hasn't been updated to point at BN1's new pod IP yet, so
#       in-cluster ClusterIP-based traffic (what BN2/BN3 use) is routed
#       nowhere or to a dead IP, while port-forward's direct-to-pod-IP path
#       is unaffected; or
#   (b) something that also breaks a direct pod-IP connection (e.g. BN1
#       itself refusing/dropping in-cluster peer connections regardless of
#       routing path), which would rule out Endpoints staleness; or
#   (c) neither — raw TCP connects fine both ways, meaning the bug is above
#       the TCP layer entirely (TLS/HTTP2/gRPC handshake), not networking.
#
# This is a read-only diagnostic: it does not change any cluster state. Run
# it manually against a live `task up` (or CI) cluster right after a BN1
# restart, while BN2/BN3 are still failing their TSS polls.
#
# Usage:
#     diagnose-bn1-post-restart-connectivity.sh [<from-bn-index> ...]
#     diagnose-bn1-post-restart-connectivity.sh        # defaults to "2 3"
#     diagnose-bn1-post-restart-connectivity.sh 2
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#
# BN1's gRPC port is read from its own "-config" ConfigMap (SERVER_PORT),
# falling back to 40840 only if that key is absent — same derivation as
# reconfigure-bn-roster-bootstrap-tss.sh.

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"

log() { echo "[wrb-dist-diagnose-bn1] $*"; }
fail() { echo "[wrb-dist-diagnose-bn1] ERROR: $*" >&2; exit 1; }

from_indices=("$@")
[[ ${#from_indices[@]} -ge 1 ]] || from_indices=(2 3)

kctl() {
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" "$@"
}

bn1="block-node-1"
bn1_pod="${bn1}-0"
bn1_dns="${bn1}.${NAMESPACE}.svc.cluster.local"

log "Resolving BN1's configured gRPC port from ${bn1}-config ConfigMap..."
bn1_port=$(kctl get configmap "${bn1}-config" -o jsonpath='{.data.SERVER_PORT}' 2>/dev/null || echo "")
: "${bn1_port:=40840}"
log "BN1 gRPC port: ${bn1_port}"

log "Fetching ${bn1_pod}'s current pod IP..."
bn1_pod_ip=$(kctl get pod "${bn1_pod}" -o jsonpath='{.status.podIP}' 2>/dev/null || echo "")
[[ -n "${bn1_pod_ip}" ]] || fail "Could not read podIP for ${bn1_pod} (is it Running?)"
log "${bn1_pod}'s current pod IP: ${bn1_pod_ip}"

log "Fetching ${bn1} Service's current Endpoints..."
bn1_endpoint_ips=$(kctl get endpoints "${bn1}" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || echo "")
log "${bn1} Endpoints IP(s): [${bn1_endpoint_ips:-<empty>}]"

endpoints_fresh="false"
if [[ -n "${bn1_endpoint_ips}" ]] && grep -qw "${bn1_pod_ip}" <<<"${bn1_endpoint_ips}"; then
    endpoints_fresh="true"
    log "VERDICT: Endpoints object matches ${bn1_pod}'s current IP (fresh)."
else
    log "VERDICT: Endpoints object does NOT contain ${bn1_pod}'s current IP (${bn1_pod_ip}) — stale or empty."
fi

# Single-shot raw TCP connect test, run remotely inside a BN pod via
# `kubectl exec`. Uses bash's own /dev/tcp (not nc/curl/grpcurl) because the
# block-node-server image (eclipse-temurin JRE on UBI-minimal) sets
# /bin/bash as its login shell, so real GNU bash — and therefore /dev/tcp —
# is guaranteed present, unlike a busybox/ash-based image.
tcp_probe() {
    local from_pod="$1" target_host="$2" target_port="$3" label="$4"
    local remote_script
    remote_script=$(cat <<EOF
if timeout 5 bash -c 'exec 3<>/dev/tcp/${target_host}/${target_port}' 2>/tmp/.wrb_diag_tcp_err; then
    echo "CONNECT_OK"
else
    echo "CONNECT_FAIL: \$(tr '\n' ' ' </tmp/.wrb_diag_tcp_err 2>/dev/null)"
fi
rm -f /tmp/.wrb_diag_tcp_err
EOF
)
    local result
    result=$(kctl exec "${from_pod}" -c block-node-server -- bash -c "${remote_script}" 2>&1) || true
    log "  [${label}] ${from_pod} -> ${target_host}:${target_port}: ${result}"
    [[ "${result}" == *CONNECT_OK* ]]
}

for from_index in "${from_indices[@]}"; do
    from_bn="block-node-${from_index}"
    from_pod="${from_bn}-0"
    log "Probing from ${from_pod}..."

    via_service_ok="false"
    tcp_probe "${from_pod}" "${bn1_dns}" "${bn1_port}" "via Service DNS" && via_service_ok="true"

    via_pod_ip_ok="false"
    tcp_probe "${from_pod}" "${bn1_pod_ip}" "${bn1_port}" "via direct pod IP" && via_pod_ip_ok="true"

    if [[ "${via_service_ok}" == "true" && "${via_pod_ip_ok}" == "true" ]]; then
        log "${from_bn}: both paths connect at the TCP level. If the Java client still fails, look above TCP (TLS/HTTP2/gRPC), not networking."
    elif [[ "${via_service_ok}" == "false" && "${via_pod_ip_ok}" == "true" ]]; then
        log "${from_bn}: Service DNS path FAILS but direct pod-IP path WORKS — points at Service/Endpoints routing (endpoints_fresh=${endpoints_fresh})."
    elif [[ "${via_service_ok}" == "false" && "${via_pod_ip_ok}" == "false" ]]; then
        log "${from_bn}: BOTH paths fail at raw TCP — not a Service/Endpoints-specific issue; ${bn1_pod} itself is refusing/unreachable for in-cluster peers regardless of routing path."
    else
        log "${from_bn}: unexpected combination (service ok, direct pod IP failed) — re-run and inspect raw output above."
    fi
done

log "Done. Endpoints freshness: ${endpoints_fresh}. See per-BN verdicts above."
