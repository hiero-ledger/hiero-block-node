#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Waits until the release artifacts a Solo E2E run pulls for a Block Node version are
# retrievable: the Helm chart on ghcr.io and every plugin jar the chart resolves from Maven
# Central. A tag push starts the E2E scheduler and the release workflow at the same time,
# and even after the release workflow finishes, Maven Central keeps serving 404 for a while.
# Without this the run fails with a Helm or plugin-resolver error that looks like a deploy bug.
#
# The chart is pushed only after the container images and jars, so its presence implies the
# images are published too.
#
# Usage: ./wait-for-bn-artifacts.sh <bn_version>     (e.g. v0.41.0-rc1 or 0.41.0-rc1)
# SNAPSHOT versions are skipped: they are republished on every main push.

set -o pipefail

readonly CHART_REF="oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server"
readonly MAVEN_BASE="https://repo1.maven.org/maven2/org/hiero/block-node"
readonly WAIT_TIMEOUT_SECONDS=3600
readonly POLL_INTERVAL_SECONDS=60
readonly PROBE_TIMEOUT_SECONDS=30

function probe_chart_values {
  timeout "${PROBE_TIMEOUT_SECONDS}" helm show values "${CHART_REF}" --version "$1" 2>/dev/null
}

function probe_maven_jar {
  local name="$1" version="$2"
  curl -sfI --max-time "${PROBE_TIMEOUT_SECONDS}" -o /dev/null "${MAVEN_BASE}/${name}/${version}/${name}-${version}.jar"
}

# Prints the missing artifacts, space-separated; prints nothing when all are present.
function find_missing_bn_artifacts {
  local version="$1" values names name missing=""
  if ! values=$(probe_chart_values "${version}"); then
    echo "chart ${CHART_REF}:${version}"
    return 0
  fi
  names=$(printf '%s\n' "${values}" | sed -n 's/^  names: *"\(.*\)"$/\1/p' | head -1)
  if [[ -z "${names}" ]]; then
    echo "plugins.names in chart ${CHART_REF}:${version}"
    return 0
  fi
  for name in ${names//,/ }; do
    # A name:version entry pins its own version and is not part of this release.
    if [[ "${name}" != *:* ]] && ! probe_maven_jar "${name}" "${version}"; then
      missing+="org.hiero.block-node:${name}:${version} "
    fi
  done
  echo "${missing% }"
}

function wait_for_bn_artifacts {
  local version="${1#v}" timeout="$2" interval="$3" missing
  local deadline=$((SECONDS + timeout))
  if [[ "${version}" == *-SNAPSHOT ]]; then
    echo "Skipping artifact wait for SNAPSHOT ${version}" >&2
    return 0
  fi
  while true; do
    missing=$(find_missing_bn_artifacts "${version}")
    if [[ -z "${missing}" ]]; then
      echo "Block Node ${version} artifacts are published" >&2
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "::error::Block Node ${version} release artifacts are not published after $((timeout / 60)) minutes. Missing: ${missing}. The release workflow may still be running or may have failed; this run did not reach deployment."
      return 1
    fi
    echo "Waiting ${interval}s for: ${missing}" >&2
    sleep "${interval}"
  done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  [[ -n "${1:-}" ]] || { echo "Usage: $0 <bn_version>" >&2; exit 2; }
  wait_for_bn_artifacts "$1" "${WAIT_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}"
fi
