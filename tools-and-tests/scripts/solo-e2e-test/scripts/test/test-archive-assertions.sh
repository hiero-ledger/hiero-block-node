#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit tests for the archive assertions in solo-test-runner.sh:
#   - filter_archive_keys        (which bucket objects count as archive keys)
#   - assert_archive_files_exist (absolute count, and growth over a recorded baseline)
#   - assert_archive_contiguous  (no gap in the archived run)
#
# Runs without a cluster: overrides list_archive_keys to serve canned object keys.
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The runner parses its options at source time, so hand it a real test file; main()
# itself stays unrun (it is guarded on BASH_SOURCE).
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../solo-test-runner.sh" --test "${SCRIPT_DIR}/../../tests/archive-backfill.yaml"

# Baselines are keyed by namespace, so a private one keeps the run out of any real
# /tmp/archive-count-* file left behind by an actual test run on this machine.
NAMESPACE="unit-test-$$"
trap 'rm -f "/tmp/archive-count-${NAMESPACE}-"*' EXIT

KEYS=""
function list_archive_keys { [[ -n "${KEYS}" ]] && echo "${KEYS}"; return 0; }

passed=0
failed=0
function record {
    local name="$1" expected="$2" actual="$3"
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  PASS  ${name}"
        passed=$((passed + 1))
    else
        echo "  FAIL  ${name} (expected rc=${expected}, got rc=${actual})"
        failed=$((failed + 1))
    fi
}

function check {
    local name="$1" expected="$2" bucket="$3"
    local actual=0
    assert_archive_contiguous "${bucket}" > /dev/null || actual=1
    record "${name}" "${expected}" "${actual}"
}

# check_count <name> <expected-rc> <min_files> <min_increase> <record_baseline>
function check_count {
    local name="$1" expected="$2" min_files="$3" min_increase="$4" record_baseline="$5"
    local actual=0
    assert_archive_files_exist block-archive-tar "${min_files}" "${min_increase}" "${record_baseline}" \
        > /dev/null || actual=1
    record "${name}" "${expected}" "${actual}"
}

# n_keys <count> — canned TAR keys 0..count-1, the shape list_archive_keys returns.
function n_keys {
    local i
    for ((i = 0; i < $1; i++)); do
        printf '0000/0000/0000/0000/%d.tar\n' "${i}"
    done
}

echo "filter_archive_keys"
foreign="2026-05-01_21-32-38_0000000000000116700-0000000000000116799"
kept=$(printf '%s\n%s\n%s\n' "0000/0000/0000/0000/12.tar" "${foreign}" "0000/0000/0000/0000/13.tar" \
    | filter_archive_keys)
record "drops a foreign object, keeps the archive keys" \
    $'0000/0000/0000/0000/12.tar\n0000/0000/0000/0000/13.tar' "${kept}"

kept=$(printf '%s\n' "0000/0000/0000/0000/notanumber.tar" | filter_archive_keys)
record "drops a non-numeric last segment" "" "${kept}"

echo ""
echo "assert_archive_contiguous"

# TAR keys, grouping level 1: last segment is groupStart / 10, leading zeros stripped.
KEYS=$'0000/0000/0000/0000/8.tar\n0000/0000/0000/0000/9.tar\n0000/0000/0000/0000/10.tar\n0000/0000/0000/0000/11.tar'
check "TAR: unbroken run across the 9 -> 10 segment-width change" 0 block-archive-tar

KEYS=$'0000/0000/0000/0000/8.tar\n0000/0000/0000/0000/9.tar\n0000/0000/0000/0000/14.tar'
check "TAR: gap where the replacement node skipped ahead" 1 block-archive-tar

# Expanded keys: the whole number is the block number, last segment padded to 3.
KEYS=$'0000/0000/0000/0000/998.blk.zstd\n0000/0000/0000/0000/999.blk.zstd\n0000/0000/0000/0001/000.blk.zstd'
check "expanded: unbroken run across a segment rollover" 0 block-archive-expanded

KEYS=$'0000/0000/0000/0000/100.blk.zstd\n0000/0000/0000/0000/101.blk.zstd\n0000/0000/0000/0000/140.blk.zstd'
check "expanded: gap over the blocks produced while nothing archived" 1 block-archive-expanded

KEYS=""
check "empty bucket fails rather than passing vacuously" 1 block-archive-tar

# The filter only drops non-numeric keys, so a foreign object with numeric segments
# survives it and decodes to an unrelated number -- which surfaces as a gap. Failing
# loudly is the safe direction here, so pin the behaviour.
KEYS=$'0000/0000/0000/0000/8.tar\n0000/0000/0000/0000/9.tar\n1234/5678/90.tar'
check "foreign key that survives the filter is reported as a gap" 1 block-archive-tar

# The last-segment width is derived from the grouping level, not hard-coded per test:
# at level 2 the key holds groupStart / 100 and the last segment is 1 char wide, so
# 0000/0000/0000/0001/2 decodes to 12 (blocks 1200-1299), not 1.
ARCHIVE_GROUPING_LEVEL=2
KEYS=$'0000/0000/0000/0001/1.tar\n0000/0000/0000/0001/2.tar\n0000/0000/0000/0001/3.tar'
check "TAR: grouping level 2 uses a 1-char last segment" 0 block-archive-tar

KEYS=$'0000/0000/0000/0001/1.tar\n0000/0000/0000/0001/5.tar'
check "TAR: grouping level 2 still detects a gap" 1 block-archive-tar
ARCHIVE_GROUPING_LEVEL=1

echo ""
echo "assert_archive_files_exist"

KEYS=$(n_keys 3)
check_count "absolute count: 3 files meets min_files 3" 0 3 0 false
check_count "absolute count: 3 files misses min_files 4" 1 4 0 false

# No baseline recorded yet: growth cannot be evaluated, so it must fail rather than
# silently degrading into "bucket is non-empty".
check_count "min_increase without a baseline fails" 1 1 1 false

# Only record_baseline=true writes the baseline.
check_count "record_baseline stores the count" 0 1 0 true
KEYS=$(n_keys 3)
check_count "min_increase 1 fails when nothing was added" 1 1 1 false
KEYS=$(n_keys 4)
check_count "min_increase 1 passes on one new object" 0 1 1 false

# A non-recording check between baseline and assertion must not move the bar: the
# baseline is still 3, so 4 objects still clears 3 + 1.
KEYS=$(n_keys 10)
check_count "a non-recording check does not move the bar" 0 1 0 false
KEYS=$(n_keys 4)
check_count "bar still comes from the recorded baseline" 0 1 1 false

echo "garbage" > "/tmp/archive-count-${NAMESPACE}-block-archive-tar"
check_count "corrupt baseline fails rather than counting as zero" 1 1 1 false

echo ""
echo "Passed: ${passed}, Failed: ${failed}"
[[ ${failed} -eq 0 ]]
