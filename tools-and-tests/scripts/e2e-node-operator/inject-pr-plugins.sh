#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Replace the Maven-resolved block-node plugin jars on the kind node's plugins hostPath with the jars built
# from this checkout. The chart's resolve-plugins init container downloads plugins from Maven, so for a PR
# it gets the plugin jars published from main, not the PR's. When a PR changes the SPI this fails with
# NoSuchMethodError, because the PR's core runs against the old plugins.
#
# The init container skips resolution when /plugins/.resolved-hash matches, so after replacing the jars
# and restarting the pod the PR jars stay in place.
#
# Run in the background alongside `solo-provisioner block node upgrade`; it waits until the upgrade's
# resolve-plugins has written a new .resolved-hash, swaps the jars, then restarts the pod.
#
# Usage: inject-pr-plugins.sh <snapshot-version> [cluster=operator-test] [namespace=block-node] [release=block-node]
set -uo pipefail

SNAP="${1:?snapshot version required}"
CLUSTER_NAME="${2:-operator-test}"
NAMESPACE="${3:-block-node}"
RELEASE="${4:-block-node}"
NODE="${CLUSTER_NAME}-control-plane"
PLUGINS_DIR=/tmp/bn/plugins
PR_JARS_DIR="block-node/app/build/docker/plugins"

node() { docker exec "${NODE}" "$@"; }

BASE_HASH=$(node cat "${PLUGINS_DIR}/.resolved-hash" 2>/dev/null || true)

# ~25 minutes, a little over the upgrade --timeout.
for _ in $(seq 1 300); do
  HASH=$(node cat "${PLUGINS_DIR}/.resolved-hash" 2>/dev/null || true)
  if [[ -n "${HASH}" && "${HASH}" != "${BASE_HASH}" ]]; then
    echo "inject-pr-plugins: upgrade plugins resolved (hash ${HASH:0:12}); swapping in PR jars"
    for jar in "${PR_JARS_DIR}"/*-"${SNAP}".jar; do
      [[ -f "${jar}" ]] || continue
      name=$(basename "${jar}")
      artifact="${name%-${SNAP}.jar}"
      # Only replace modules the profile resolved; Maven names SNAPSHOT jars with the base version or a
      # timestamp, so match on the artifact id and the version's leading digits.
      if node sh -c "ls ${PLUGINS_DIR}/${artifact}-[0-9]*.jar >/dev/null 2>&1"; then
        node sh -c "rm -f ${PLUGINS_DIR}/${artifact}-[0-9]*.jar"
        # Stream the jar in: `docker cp` cannot write into the node's tmpfs-backed /tmp.
        if ! docker exec -i "${NODE}" sh -c "cat > ${PLUGINS_DIR}/${name} && chmod 644 ${PLUGINS_DIR}/${name}" < "${jar}"; then
          echo "inject-pr-plugins: failed to copy ${name}" >&2
          exit 1
        fi
        echo "inject-pr-plugins: replaced ${artifact} with ${name}"
      fi
    done
    node ls -l "${PLUGINS_DIR}"
    # init containers re-run on the new pod; the hash matches so resolve-plugins leaves the jars alone.
    kubectl delete pod -n "${NAMESPACE}" "${RELEASE}-block-node-server-0" --wait=false
    exit 0
  fi
  sleep 5
done
echo "inject-pr-plugins: timed out waiting for the upgrade to resolve plugins" >&2
exit 1
