#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — steps 11-12) — reconfigure a BN to
# fetch an RSA address book from the in-cluster Mirror Node via the
# roster-bootstrap-rsa plugin.
#
# Why: blocks produced before the CN's v0.76 TSS upgrade (cn-upgrade-tss.sh,
# step 11) are RSA-signed, not TSS-signed — TssData (from
# reconfigure-bn-roster-bootstrap-tss.sh / TSS peer gossip) can never verify
# them. Verifying an RSA-signed block requires an "address book era" covering
# its block number (RSAProofVerifier), which BN2/BN3 can only get from
# rsa-address-book-history.json, populated by this plugin. Without it,
# BackfillPlugin's live-tail fetch of pre-upgrade blocks 0..N from BN1
# succeeds at the network level but every block fails verification with
# MISSING_VERIFICATION_DATA, forever — no amount of waiting fixes this, since
# there's simply no roster source configured at all.
#
# BN1 is not a viable roster source here: it has the exact same empty
# roster.bootstrap.rsa.* config (confirmed via its own startup logs —
# "blockNodeSourcesPath is blank ... mirrorNodeBaseUrl is blank"), so peer-BN
# gRPC query (the mechanism reconfigure-bn-roster-bootstrap-tss.sh uses) has
# nothing to fetch. The Mirror Node is the only node in this topology that
# actually has a correct address book (from Solo's own genesis setup), so
# this points roster.bootstrap.rsa.mirrorNodeBaseUrl there instead — the same
# mechanism solo-deploy-network.sh's generate_bn_wrb_overlay already uses for
# genesis-time "rsa-wrb" topologies (single-wrb-rsa, 3cn-2bn-wrb-rsa). BN2/BN3
# here are added post-hoc via add-bn.sh, which never goes through that path,
# so nothing wires this up for them without this script.
#
# The plugin needs BOTH /api/v1/blocks and /api/v1/network/nodes under the
# SAME base URL, but Solo splits these across two separate services: /api/v1
# (broad prefix, includes /blocks) only exists on mirror-<n>-rest (the JS REST
# service), while /api/v1/network/ is restjava-only (rest-java 404s on
# /blocks; the JS service 404s on /network/nodes). Neither backend alone
# satisfies both calls -- confirmed live (both individually 404 on the wrong
# service). mirror-ingress-controller-<namespace> unifies them correctly:
# it path-routes /api/v1/network/ to restjava and everything else under
# /api/v1 to rest, so this points mirrorNodeBaseUrl at the ingress rather
# than at either backend directly (solo-deploy-network.sh's own
# generate_bn_wrb_overlay predates this discovery and points at restjava
# directly, which works for genesis-time rsa-wrb topologies only because they
# apparently don't hit the /blocks call this plugin also needs -- worth
# revisiting there too).
#
# roster-bootstrap-rsa is in the chart's DEFAULT plugins.names list
# (charts/block-node-server/values.yaml), so every BN in this suite already
# has its JAR loaded from install time — it's simply inactive because
# ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_BASE_URL defaults to "". Solo has no
# `solo block node update`/`config` subcommand (only `add`), so — same
# kubectl-patch philosophy as reconfigure-bn-backfill.sh /
# reconfigure-bn-roster-bootstrap-tss.sh — this reconfigures the
# already-running BN by hand instead of re-rendering the chart. Unlike those
# two, this needs no sources ConfigMap or volume/mount: mirrorNodeBaseUrl is
# a plain string value on the existing "<bn>-config" ConfigMap (already wired
# into the container via envFrom: configMapRef). A ConfigMap-only change
# doesn't trigger a rollout on its own though (env vars are baked in at
# container start), so this explicitly restarts the StatefulSet, unlike the
# other two reconfigure-*.sh scripts whose pod-template patch triggers one
# implicitly.
#
# Usage:
#     reconfigure-bn-roster-bootstrap-rsa.sh <target-bn-index> [<target-bn-index> ...]
#     reconfigure-bn-roster-bootstrap-rsa.sh 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   READY_TIMEOUT      (default 300)
#
# The Mirror Node's REST endpoint is discovered via the mirror ingress
# controller (mirror-ingress-controller-<namespace>), which path-routes
# /api/v1/network/ to rest-java and everything else under /api/v1 (including
# /blocks) to the JS rest service — both of which this plugin needs under one
# base URL (see the note above; neither backend alone satisfies both calls).

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

[[ $# -ge 1 ]] || { echo "reconfigure-bn-roster-bootstrap-rsa.sh: at least one target BN index required (e.g. 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-roster-rsa] $*"; }
fail() { echo "[wrb-dist-bn-roster-rsa] ERROR: $*" >&2; exit 1; }

mn_ingress_svc=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get svc -o name 2>/dev/null | grep -E "mirror-ingress-controller" | head -1 | sed 's#^service/##')
[[ -z "${mn_ingress_svc}" ]] && mn_ingress_svc="mirror-ingress-controller-${NAMESPACE}"
mn_base_url="http://${mn_ingress_svc}.${NAMESPACE}.svc.cluster.local"
log "Using Mirror Node base URL (via ingress): ${mn_base_url}"

for target_index in "$@"; do
    target_bn="block-node-${target_index}"
    config_configmap="${target_bn}-config"

    log "Reconfiguring ${target_bn} to fetch its RSA roster from the Mirror Node..."
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        patch configmap "${config_configmap}" --type merge -p "$(cat <<EOF
{
  "data": {
    "ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_BASE_URL": "${mn_base_url}",
    "ROSTER_BOOTSTRAP_RSA_INITIAL_QUERY_INTERVAL_MILLIS": "3000"
  }
}
EOF
)" || fail "failed to patch ${config_configmap}"
    log "  ${config_configmap} patched (ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_BASE_URL)."

    log "Restarting ${target_bn} so the new env var takes effect (ConfigMap changes alone don't trigger a rollout)..."
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        rollout restart statefulset/"${target_bn}" \
        || fail "rollout restart failed for statefulset/${target_bn}"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        rollout status statefulset/"${target_bn}" --timeout="${READY_TIMEOUT}s" \
        || {
            kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
                describe pod/"${target_bn}-0" | tail -30 || true
            fail "${target_bn} rollout did not complete"
        }

    log "${target_bn} is now configured to fetch its RSA roster from ${mn_base_url}."
done
