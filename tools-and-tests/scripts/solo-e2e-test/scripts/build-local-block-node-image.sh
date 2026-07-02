#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Builds a Block Node Docker image from local source, with all plugins (including
# verification) baked in from the current checkout, and loads it into a Kind cluster.
#
# This exists because solo-e2e-test's verification-plugin work targets a rewritten
# `verification` module that is not published to Maven Central (see
# gradle/aggregation/build.gradle.kts) — the chart's Maven-based plugin resolution has
# nothing to download. Instead of relying on a published image + Maven-resolved plugin
# jars, this script builds the `solo-dev` Docker target (base image + all locally-built
# plugin jars baked in) and loads it into the Kind cluster so it can be referenced
# directly via `solo block node add --image-tag`.
#
# Usage:
#   ./build-local-block-node-image.sh --cluster-name NAME [--tag TAG]
#
# Options:
#   --cluster-name NAME   Kind cluster name to load the image into (required)
#   --tag TAG             Image tag to build/load (default: repo-root version.txt contents)
#   --help                Show this help message
#
# Output:
#   Prints "bn_image_tag=<TAG>" on stdout on success, for capture by the caller.

set -o pipefail
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

function fail {
    printf '%s\n' "$1" >&2
    exit "${2-1}"
}

function show_help {
  cat << EOF
Usage: $(basename "$0") --cluster-name NAME [--tag TAG]

Builds a Block Node image from local source (all plugins baked in) and loads it into
a Kind cluster.

Options:
  --cluster-name NAME   Kind cluster name to load the image into (required)
  --tag TAG             Image tag to build/load (default: repo-root version.txt contents)
  --help                Show this help message
EOF
  exit 0
}

CLUSTER_NAME=""
TAG=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --cluster-name)
      CLUSTER_NAME="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
      ;;
    --help|-h)
      show_help
      ;;
    *)
      fail "Unknown option: $1. Use --help for usage information." 1
      ;;
  esac
done

[[ -z "${CLUSTER_NAME}" ]] && fail "ERROR: --cluster-name is required" 1

if [[ -z "${TAG}" ]]; then
  TAG="$(cat "${REPO_ROOT}/version.txt")"
fi

readonly BASE_IMAGE_NAME="block-node-server"
readonly SOLO_DEV_IMAGE_NAME="block-node-server-solo-dev"

echo "Building Block Node image [${BASE_IMAGE_NAME}:${TAG}] from local source (all plugins baked in)..." >&2

GRADLE="${REPO_ROOT}/gradlew"

"${GRADLE}" -p "${REPO_ROOT}" :app:clean :app:assemble \
  || fail "ERROR: Gradle assemble failed" 1
"${GRADLE}" -p "${REPO_ROOT}" :app:copyDockerFolder \
  || fail "ERROR: Gradle copyDockerFolder failed" 1
"${GRADLE}" -p "${REPO_ROOT}" :app:prepareDockerPlugins \
  || fail "ERROR: Gradle prepareDockerPlugins failed" 1
"${GRADLE}" -p "${REPO_ROOT}" :app:createDockerImage \
  || fail "ERROR: Gradle createDockerImage failed" 1

# Solo's `--image-tag` flag checks for an image under the plain "block-node-server" name,
# so the plugins-baked-in solo-dev image is retagged to that name for the given TAG.
docker tag "${SOLO_DEV_IMAGE_NAME}:${TAG}" "${BASE_IMAGE_NAME}:${TAG}" \
  || fail "ERROR: Failed to tag ${SOLO_DEV_IMAGE_NAME}:${TAG} as ${BASE_IMAGE_NAME}:${TAG}" 1

echo "Loading [${BASE_IMAGE_NAME}:${TAG}] into Kind cluster '${CLUSTER_NAME}'..." >&2
kind load docker-image "${BASE_IMAGE_NAME}:${TAG}" --name "${CLUSTER_NAME}" \
  || fail "ERROR: kind load docker-image failed" 1

if ! docker exec "${CLUSTER_NAME}-control-plane" crictl images 2>/dev/null \
    | grep -q "${BASE_IMAGE_NAME}.*${TAG}"; then
  fail "ERROR: local Block Node image ${BASE_IMAGE_NAME}:${TAG} not found inside Kind node after 'kind load'" 1
fi

echo "Local Block Node image [${BASE_IMAGE_NAME}:${TAG}] ready in cluster '${CLUSTER_NAME}'." >&2
echo "bn_image_tag=${TAG}"
