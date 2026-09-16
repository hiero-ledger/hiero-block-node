#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# `timeout --foreground` signals only this script, never the helper. If the caller reads
# this output through a command substitution it then waits on the helper's copy of the
# pipe -- the killed monitor still wedges the caller until the helper exits. The caller
# must collect the output some other way; that is what the test asserts.

# The leak: deliberately never killed, and holds whatever stdout it was given.
sleep 300 &

# Bash defers a trap until the foreground command finishes, so the hang is backgrounded
# and waited on -- otherwise TERM would not be handled for the full 300s and every caller
# would look wedged, leak or no leak.
trap 'kill "${child}" 2>/dev/null; exit 143' TERM
sleep 300 &
child=$!
wait "${child}"
