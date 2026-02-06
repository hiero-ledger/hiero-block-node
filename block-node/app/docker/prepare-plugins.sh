#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Prepares plugins for local docker-compose deployment.
# This script builds the plugins and copies them to the docker plugins directory.
#
# Usage:
#   ./prepare-plugins.sh           # Prepare all plugins (default)
#   ./prepare-plugins.sh all       # Prepare all plugins
#   ./prepare-plugins.sh minimal   # Prepare minimal plugins
#   ./prepare-plugins.sh lfh       # Prepare Local File History plugins
#   ./prepare-plugins.sh rfh       # Prepare Remote File History plugins
#
# After running this script, start the container with:
#   docker compose up

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../../.."
PLUGINS_DIR="${SCRIPT_DIR}/plugins"
PROFILE="${1:-all}"

echo "=== Preparing plugins for profile: ${PROFILE} ==="
echo ""

# Define plugins per profile
case "${PROFILE}" in
    minimal)
        PLUGINS="facility-messaging health server-status stream-publisher stream-subscriber verification blocks-file-recent"
        ;;
    lfh)
        PLUGINS="facility-messaging health server-status block-access-service stream-publisher stream-subscriber verification blocks-file-recent blocks-file-historic backfill"
        ;;
    rfh)
        PLUGINS="facility-messaging health server-status block-access-service stream-publisher stream-subscriber verification blocks-file-recent s3-archive"
        ;;
    all|*)
        PLUGINS="facility-messaging health server-status block-access-service stream-publisher stream-subscriber verification blocks-file-recent blocks-file-historic backfill s3-archive"
        ;;
esac

# Build plugins
echo "Building plugins..."
cd "${PROJECT_ROOT}"

# Build all plugin modules in parallel
PLUGIN_TASKS=()
for plugin in ${PLUGINS}; do
    PLUGIN_TASKS+=( ":${plugin}:jar" )
done
./gradlew "${PLUGIN_TASKS[@]}" --parallel -q

# Clean and create plugins directory
echo ""
echo "Copying plugins to ${PLUGINS_DIR}..."
rm -rf "${PLUGINS_DIR}"
mkdir -p "${PLUGINS_DIR}"

# Copy plugin jars
for plugin in ${PLUGINS}; do
    jar_path=$(find "block-node/${plugin}/build/libs" -name "${plugin}-*.jar" \
        -not -name "*-sources.jar" \
        -not -name "*-javadoc.jar" \
        -not -name "*-test-fixtures.jar" \
        2>/dev/null | head -1)
    if [[ -f "$jar_path" ]]; then
        cp "$jar_path" "${PLUGINS_DIR}/"
        echo "  ✓ $(basename "$jar_path")"
    else
        echo "  ✗ ${plugin} (not found)"
    fi
done

echo ""
echo "=== Plugins ready ==="
echo ""
echo "To start the block node:"
echo "  cd ${SCRIPT_DIR}"
echo "  ./update-env.sh <version> [debug] [ci]"
echo "  docker compose up"
