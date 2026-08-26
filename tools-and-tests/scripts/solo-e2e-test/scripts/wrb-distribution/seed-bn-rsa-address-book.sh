#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6) — seed-bn-rsa-address-book.
#
# Generates a correct rsa-bootstrap-roster.json by extracting RSA signing keys
# directly from CN pods and writes it to each target BN's persistent
# application-state volume BEFORE the pod starts. BlockNodeApp.loadApplicationState()
# reads that file at JVM startup (before any plugin initialises), and
# checkForApplicationStateUpdates() (called synchronously before startPlugins())
# converts it into a synthetic era spanning startBlock=0, endBlock=-1 (all blocks),
# so the full addressBookIndex is populated before BackfillPlugin fires its first
# verification — no race.
#
# WHY NOT the Mirror Node REST API?
# The Mirror Node's /api/v1/network/nodes returns RSA keys from Helm chart values
# that do not match the actual signing keys used by CN nodes to produce record
# file signatures (confirmed by extract-solo-ab-and-generate.sh line 18:
# "The key stored in mirror Helm values doesn't match the actual signing key").
# Using those keys causes ExtendedMerkleTreeSession.verifyRsaProof to produce
# validCount=0 for every block >= 17 (the HAPI >=0.72 boundary), permanently
# stalling BackfillPlugin at block 16 regardless of how many retries run.
#
# Usage:
#     seed-bn-rsa-address-book.sh <target-bn-index>...
#     e.g.: seed-bn-rsa-address-book.sh 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   READY_TIMEOUT      (default 300)
#   ENV_FILE           (default /tmp/wrb-distribution-step12.env)

set -euo pipefail

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log()  { echo "[wrb-dist-seed-rsa] $*"; }
fail() { echo "[wrb-dist-seed-rsa] ERROR: $*" >&2; exit 1; }

# --------------------------------------------------------------------------- #
# Arguments                                                                     #
# --------------------------------------------------------------------------- #

if [[ $# -lt 1 ]]; then
    fail "Usage: $0 <target-bn-index>..."
fi

target_indices=("$@")
rsa_file="/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json"
local_seed="/tmp/wrb-dist-rsa-bootstrap-seed.json"

# --------------------------------------------------------------------------- #
# Extract RSA signing keys from CN pods                                         #
# --------------------------------------------------------------------------- #

log "Discovering CN pods in namespace ${NAMESPACE}..."
all_pods=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get pods --no-headers 2>/dev/null \
    | grep "network-node" | awk '{print $1}' || true)

[[ -n "${all_pods}" ]] || fail "No network-node pods found in namespace ${NAMESPACE}"

node_addresses_json="[]"
while IFS= read -r pod_name; do
    [[ -z "${pod_name}" ]] && continue

    node_num=$(echo "${pod_name}" | grep -oE 'node[0-9]+' | grep -oE '[0-9]+' | head -1 || true)
    if [[ -z "${node_num}" ]]; then
        log "  WARNING: could not parse node number from ${pod_name}, skipping"
        continue
    fi

    # Solo pod names are 1-indexed (network-node1/2/3) but Hedera nodeId is
    # 0-indexed — network-node1 has nodeId=0 (confirmed against /api/v1/network/nodes).
    node_id=$(( node_num - 1 ))
    node_account_id=$(( node_num + 2 ))
    log "Extracting RSA signing key from ${pod_name} (nodeId=${node_id})..."

    rsa_hex=""
    for key_file in \
        "s-public-node${node_num}.pem" \
        "s-public-node${node_account_id}.pem" \
        "s-public.pem" \
        "a-public-node${node_num}.pem" \
        "a-public-node${node_account_id}.pem" \
        "a-public.pem" \
        "public.pem"
    do
        pem_content=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            exec "${pod_name}" -- \
            cat "/opt/hgcapp/services-hedera/HapiApp2.0/data/keys/${key_file}" 2>/dev/null || true)
        [[ -z "${pem_content}" ]] && continue

        if echo "${pem_content}" | grep -q "BEGIN CERTIFICATE"; then
            # Extract SubjectPublicKeyInfo (raw public key) from the certificate.
            rsa_hex=$(echo "${pem_content}" \
                | openssl x509 -pubkey -noout 2>/dev/null \
                | grep -v "BEGIN\|END" | tr -d '\n' \
                | base64 -d 2>/dev/null \
                | od -An -tx1 | tr -d ' \n' || true)
        else
            # PEM-encoded public key — decode base64 to binary, then hex-encode.
            rsa_hex=$(echo "${pem_content}" \
                | grep -v "BEGIN\|END" | tr -d '\n' \
                | base64 -d 2>/dev/null \
                | od -An -tx1 | tr -d ' \n' || true)
        fi

        if [[ -n "${rsa_hex}" ]]; then
            log "  Found key in ${key_file}"
            break
        fi
    done

    if [[ -z "${rsa_hex}" ]]; then
        log "  WARNING: Could not extract RSA key from ${pod_name}, skipping node ${node_id}"
        continue
    fi

    entry=$(jq -n --argjson nodeId "${node_id}" --arg rsaKey "${rsa_hex}" \
        '{"nodeId": $nodeId, "RSAPubKey": $rsaKey}')
    node_addresses_json=$(echo "${node_addresses_json}" | jq --argjson e "${entry}" '. + [$e]')
done <<< "${all_pods}"

node_count=$(echo "${node_addresses_json}" | jq 'length')
[[ "${node_count}" -gt 0 ]] \
    || fail "Could not extract any RSA keys from CN pods — check that pods are Running and key files exist"
log "Extracted ${node_count} RSA signing key(s) from CN pods."

# Generate legacy NodeAddressBook format. BlockNodeApp.loadApplicationState() detects
# the missing "addressBooks" field, falls back to the NodeAddressBook parser, and wraps
# the result into a synthetic era with startBlock=0, endBlock=-1 (open-ended), so
# getAddressBookForBlock(N) returns a non-null era for every block number N including
# the block-17 HAPI-version boundary where ExtendedMerkleTreeSession first kicks in.
jq -n --argjson addrs "${node_addresses_json}" '{"nodeAddress": $addrs}' > "${local_seed}"

local_size=$(wc -c < "${local_seed}" | tr -d ' ')
log "Generated RSA address book (${node_count} node(s), ${local_size} bytes) at ${local_seed}."

# --------------------------------------------------------------------------- #
# Seed each target BN                                                           #
# --------------------------------------------------------------------------- #

for target_index in "${target_indices[@]}"; do
    target_pod="block-node-${target_index}-0"
    target_sts="block-node-${target_index}"
    app_state_dir="/opt/hiero/block-node/application-state"

    log "Seeding ${rsa_file} onto ${target_pod}..."

    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        exec "${target_pod}" -c block-node-server -- mkdir -p "${app_state_dir}" \
        || fail "Failed to ensure ${app_state_dir} exists on ${target_pod}"

    # Pipe the locally-generated file into the pod via tee (same pattern as
    # stage-tss-data-on-bn1.sh). The application-state directory is mounted
    # from a PVC, so the file survives the upcoming pod restart.
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        exec -i "${target_pod}" -c block-node-server -- tee "${rsa_file}" \
        < "${local_seed}" > /dev/null \
        || fail "Failed to write ${rsa_file} onto ${target_pod}"

    log "RSA address book staged onto ${target_pod}'s persistent application-state volume."

    log "Rolling ${target_sts} so it loads the pre-seeded address book at startup..."
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        rollout restart "statefulset/${target_sts}" \
        || fail "rollout restart failed for statefulset/${target_sts}"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        rollout status "statefulset/${target_sts}" --timeout="${READY_TIMEOUT}s" \
        || fail "statefulset/${target_sts} rollout did not complete in ${READY_TIMEOUT}s"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        wait --for=condition=Ready "pod/${target_pod}" --timeout="${READY_TIMEOUT}s" \
        || fail "${target_pod} did not become Ready after restart"

    log "${target_sts} restarted; BackfillPlugin will find correct RSA signing keys pre-loaded at startup."
done

log "RSA address book seeded onto BN(s) ${target_indices[*]}; done."
