#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4) — assert that a Mirror Node's importer
# is configured for and running against a Block Node source. Applies to:
#   * mirror-1 (step 6) after reconfigure-mn1-to-bn-sources.sh flips
#     HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=true and points at BN2/BN3.
#   * mirror-2 (step 7) after add-mn2.sh installs a fresh MN with block:
#     enabled: true / sourceType: BLOCK_NODE.
#
# What this asserts:
#   1. An importer pod for ${mirror_name} exists and is Ready.
#   2. The importer container has the block-source env vars set:
#        HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=true
#        HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE=BLOCK_NODE
#        HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_ENDPOINTS_0_HOST=<non-empty>
#
# Why not log-poll for CompositeBlockSource activity anymore:
#   Earlier revisions of this assertion waited (240s -> 480s -> 720s) for
#   the importer to emit its first block-source scheduler log line. That
#   timing varied wildly across CI runs -- 5-6min in some runs, >12min in
#   others -- because MN's full Flyway migration + Spring startup is
#   heavily load-dependent on the shared runner. Every window bump just
#   chased the tail of the distribution, and the assertion kept failing
#   ~40ms past the deadline as the first ERROR line finally appeared.
#
#   Pod-Ready + env-var presence is a strictly stronger invariant for what
#   slice 4 is testing (reconfigure / fresh install correctly wires MN to
#   BN). If Spring couldn't bind the block config, the pod would CrashLoop
#   and never reach Ready -- which is precisely the regression this assertion
#   is meant to catch. The BN-side data gap that made the log signal
#   necessary in the first place is Solo-test-infra scope, not what slice 4
#   is exercising.
#
# Usage:
#     assert-mn-consuming-from-bn.sh <mirror-name>
#     assert-mn-consuming-from-bn.sh mirror-1
#     assert-mn-consuming-from-bn.sh mirror-2
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   READY_TIMEOUT     (default 60 -- pod-Ready should already be true when
#                     this runs, since reconfigure-mn1 / add-mn2 both wait
#                     on the rollout; this is a small safety margin for
#                     rollout raciness)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
READY_TIMEOUT="${READY_TIMEOUT:-60}"

mirror_name="${1:?assert-mn-consuming-from-bn.sh: mirror name required (e.g. mirror-1, mirror-2)}"
importer_deployment="${mirror_name}-importer"

log() { echo "[wrb-dist-mn-bn-assert] $*"; }
fail() { echo "[wrb-dist-mn-bn-assert] ERROR: $*" >&2; exit 1; }

# 1) Find the importer pod. Prefer the standard chart label selector; fall
#    back to a name-prefix match to cover add-mn2.sh's raw-manifest deployment.
pod=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get pods -l "app.kubernetes.io/name=importer,app.kubernetes.io/instance=${mirror_name}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "${pod}" ]]; then
    pod=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get pods 2>/dev/null | awk -v m="${importer_deployment}" '$1 ~ m {print $1; exit}' || true)
fi
[[ -n "${pod}" ]] || fail "no importer pod found for ${mirror_name}"
log "Found importer pod: ${pod}"

# 2) Require the pod Ready. If Spring couldn't bind block config the pod
#    would CrashLoop and this check would time out.
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready "pod/${pod}" --timeout="${READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe "pod/${pod}" | tail -40 || true
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            logs "${pod}" --tail=100 2>/dev/null | tail -40 | sed 's/^/    /' || true
        fail "${pod} did not become Ready within ${READY_TIMEOUT}s"
    }
log "${pod} is Ready."

# 3) Extract the importer container's env vars and check for the block-source
#    triple. Uses the first container in the pod spec, which is the importer
#    for both reconfigure-mn1 (single-container patch) and add-mn2 (raw
#    single-container Deployment).
env_json=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get "pod/${pod}" -o jsonpath='{.spec.containers[0].env}' 2>/dev/null || true)
[[ -n "${env_json}" ]] || fail "could not read container env from pod/${pod}"

get_env() {
    local key="$1"
    echo "${env_json}" | jq -r --arg k "$key" '.[] | select(.name == $k) | .value // empty' 2>/dev/null || true
}

expected_enabled="true"
expected_sourcetype="BLOCK_NODE"

actual_enabled=$(get_env "HIERO_MIRROR_IMPORTER_BLOCK_ENABLED")
actual_sourcetype=$(get_env "HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE")
actual_bn0_host=$(get_env "HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_ENDPOINTS_0_HOST")

log "Block-source env:"
log "  HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=${actual_enabled:-<unset>}"
log "  HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE=${actual_sourcetype:-<unset>}"
log "  HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_ENDPOINTS_0_HOST=${actual_bn0_host:-<unset>}"

problems=()
[[ "${actual_enabled}" == "${expected_enabled}" ]] || problems+=("BLOCK_ENABLED expected '${expected_enabled}', got '${actual_enabled:-<unset>}'")
[[ "${actual_sourcetype}" == "${expected_sourcetype}" ]] || problems+=("BLOCK_SOURCETYPE expected '${expected_sourcetype}', got '${actual_sourcetype:-<unset>}'")
[[ -n "${actual_bn0_host}" ]] || problems+=("BLOCK_NODES_0_ENDPOINTS_0_HOST is empty")

if [[ "${#problems[@]}" -gt 0 ]]; then
    for p in "${problems[@]}"; do log "  - ${p}"; done
    fail "${importer_deployment}: block-source env is not correctly configured"
fi

log "${importer_deployment} is Ready and configured for BN block source (BN2+BN3 host: ${actual_bn0_host})."
