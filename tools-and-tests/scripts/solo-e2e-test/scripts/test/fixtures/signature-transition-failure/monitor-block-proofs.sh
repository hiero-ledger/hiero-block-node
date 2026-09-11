#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Stand-in for the real monitor-block-proofs.sh: fails immediately without
# hanging, so the non-timeout branch of assert_signature_transition's status
# handling gets exercised.
exit 1
