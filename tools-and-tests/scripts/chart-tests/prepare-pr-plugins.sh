#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Make the chart install tests use the PR's build of any plugin that is not on Maven yet.
#
# The chart's resolve-plugins init container downloads plugins from Maven, so a plugin that is new in a PR cannot
# be resolved until main publishes it, and the install fails. This script finds the plugins the chart tests list
# in `plugins.names` that are not published at <version> (see lib/maven-published.sh) and, for those only,
# edits the chart in the CI workspace (nothing is committed):
#   1. removes them from `plugins.names` in values.yaml and the ci/ values, so Maven is not asked for them, and
#   2. adds an init container to the default `blockNode.initContainers` in values.yaml that copies their jars from
#      the PR's locally built solo-dev image (which bakes the PR's plugins) into the plugins volume.
#      resolve-plugins only removes the jars it resolved itself, so the copied jars are kept.
# The ci/ values do not set initContainers, so every test inherits the default. Plugins that Maven does have are
# left alone, so the tests keep proving that the published artifacts resolve. When nothing is missing the chart is
# not touched at all.
#
# Usage: prepare-pr-plugins.sh <version> [kind-cluster=chart-testing]
set -euo pipefail

VERSION="${1:?version required}"
KIND_CLUSTER="${2:-chart-testing}"
SOLO_DEV_IMAGE="block-node-server-solo-dev:${VERSION}"
CHART=charts/block-node-server
VALUES="${CHART}/values.yaml"

# shellcheck source=../lib/maven-published.sh
source "$(dirname "${BASH_SOURCE[0]}")/../lib/maven-published.sh"

VALUES_FILES=()
while IFS= read -r file; do VALUES_FILES+=("${file}"); done < <(
  find "${CHART}" -maxdepth 3 -name '*.yaml' \( -name values.yaml -o -path '*/ci/*' \) | sort)

# Plugins the chart tests would ask Maven for, that Maven does not have at this version. Entries pinned with an
# explicit version (name:version) are third-party style pins and are left alone.
PR_PLUGINS=()
while IFS= read -r name; do
  [[ -z "${name}" || "${name}" == *:* ]] && continue
  if ! is_published "${name}" "${VERSION}"; then PR_PLUGINS+=("${name}"); fi
done < <(
  grep -h -E '^[[:space:]]*names:' "${VALUES_FILES[@]}" \
    | sed -E 's/.*names:[[:space:]]*"?([^"]*)"?.*/\1/' | tr ',' '\n' | sort -u)

if [[ ${#PR_PLUGINS[@]} -eq 0 ]]; then
  echo "prepare-pr-plugins: every listed plugin is published at ${VERSION}; chart left unchanged"
  exit 0
fi
echo "prepare-pr-plugins: using the PR build of: ${PR_PLUGINS[*]}"

# 1. drop the unpublished plugins from the names Maven resolves
for plugin in "${PR_PLUGINS[@]}"; do
  perl -pi -e "s/\\b${plugin},//g; s/,${plugin}\\b//g" "${VALUES_FILES[@]}"
done

# 2. copy the PR-built jars in with an init container
COPY_CMDS=""
for plugin in "${PR_PLUGINS[@]}"; do
  COPY_CMDS+="cp /opt/hiero/block-node/app-${VERSION}/plugins/${plugin}-*.jar /plugins/ && "
done
python3 - "${VALUES}" "${SOLO_DEV_IMAGE}" "${COPY_CMDS}" <<'PY'
import sys

path, image, copy_cmds = sys.argv[1:4]
anchor = "        - name: logging-storage\n          mountPath: /logging-pvc\n"
block = f"""    - name: copy-pr-plugins
      image: {image}
      imagePullPolicy: Never
      command:
        - sh
        - -c
        - {copy_cmds}chmod 644 /plugins/*.jar
      volumeMounts:
        - name: plugins-storage
          mountPath: /plugins
"""
with open(path) as f:
    text = f.read()
if text.count(anchor) != 1:
    sys.exit(f"expected exactly one init-storage-dirs anchor in {path}, found {text.count(anchor)}")
with open(path, "w") as f:
    f.write(text.replace(anchor, anchor + block))
PY
echo "updated ${VALUES}"

kind load docker-image "${SOLO_DEV_IMAGE}" --name "${KIND_CLUSTER}"
