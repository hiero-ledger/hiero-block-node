#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Stand-in for the real monitor-block-proofs.sh: hangs the way a port-forward to a
# Service with no endpoints does, so assert_signature_transition's `timeout` wrapper
# is the only thing that can end it. `timeout --foreground` only signals this direct
# child, not its descendants, so the sleep is backgrounded and explicitly killed on
# TERM here, the same way real grpcurl self-bounds via -max-time.

trap 'kill "$child" 2>/dev/null; exit 143' TERM
sleep 300 &
child=$!
wait "$child"
