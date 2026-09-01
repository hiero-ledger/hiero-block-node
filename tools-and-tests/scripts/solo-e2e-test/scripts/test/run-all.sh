#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Run every fixture-based unit test for the Solo E2E harness scripts. No cluster needed;
# each suite stubs out its cluster access. Requires yq (the runner parses options with it).
#
# Usage: scripts/test/run-all.sh   (or: task test:unit)

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

failed=0
for suite in "${SCRIPT_DIR}"/test-*.sh; do
    echo "=== $(basename "${suite}")"
    bash "${suite}" || failed=$((failed + 1))
    echo ""
done

if [[ ${failed} -gt 0 ]]; then
    echo "${failed} suite(s) failed"
    exit 1
fi
echo "All suites passed"
