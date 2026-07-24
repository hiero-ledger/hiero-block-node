#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 8) — assert each CN's
# block-nodes.json references BN3.
#
# BN3 has EMB=100_000_000 (step 5), so it will not persist blocks at the
# numbers the CN is currently producing — this only asserts the config
# surface (that reconfigure-cn-to-push-bn3.sh's write took effect on all 3
# CN pods), not that BN3 has received blocks.
#
# Reads:
#   NAMESPACE   (default "solo-network")
#   CONTEXT     (default "kind-solo-cluster")
#   BN_HOST_3   (default block-node-3.${NAMESPACE}.svc.cluster.local)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"

log() { echo "[wrb-dist-assert-cn-bn3] $*"; }
fail() { echo "[wrb-dist-assert-cn-bn3] ERROR: $*" >&2; exit 1; }

CONFIG_PATH="/opt/hgcapp/services-hedera/HapiApp2.0/data/config/block-nodes.json"

failures=0
for cn_name in node1 node2 node3; do
    cn_pod="network-${cn_name}-0"
    config_content=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        exec "${cn_pod}" -c root-container -- cat "${CONFIG_PATH}" 2>/dev/null || echo "")

    if [[ -z "${config_content}" ]]; then
        log "${cn_pod}: could not read ${CONFIG_PATH}"
        failures=$(( failures + 1 ))
        continue
    fi

    if echo "${config_content}" | grep -q "\"${BN_HOST_3}\""; then
        log "${cn_pod}: block-nodes.json references ${BN_HOST_3} ✓"
    else
        log "${cn_pod}: block-nodes.json does NOT reference ${BN_HOST_3}:"
        log "${config_content}"
        failures=$(( failures + 1 ))
    fi
done

if (( failures > 0 )); then
    fail "${failures} CN(s) did not have block-nodes.json pointing at BN3"
fi

log "All 3 CNs' block-nodes.json reference BN3 (${BN_HOST_3})."
