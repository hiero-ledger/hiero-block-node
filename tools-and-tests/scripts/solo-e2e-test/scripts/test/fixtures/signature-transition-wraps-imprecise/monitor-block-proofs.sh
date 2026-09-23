#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Stand-in for the real monitor-block-proofs.sh: reproduces what check_hard_timeout
# prints when its deadline lapses after WRAPS was already observed -- the transition is
# real, only the exact block went unnarrowed, so it exits 0 rather than 3.
#
# Pins the contract the runner depends on: that `First WRAPS block:` is still the last
# field on its own line once the caveat is added, so the summary reports a block number.
echo ""
echo "=== Signature Transition ==="
echo "  First WRAPS block: 1"
echo "  (upper bound only: exact transition block not narrowed, 4s deadline hit during binary search)"
