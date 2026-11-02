#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Shared by the fake grpcurl fixtures (fixtures/monitor-*/grpcurl): prints a getBlock
# response the way grpcurl renders it, with one block proof whose kind and signature
# size the fixture chooses.
#
# Usage:
#   fake-block-json.sh signed <signature-bytes>   TSS signed block proof
#   fake-block-json.sh state-proof                block state proof (no TSS signature)

set -euo pipefail

python3 - "$1" "${2:-0}" <<'PY'
import base64
import json
import sys

kind, size = sys.argv[1], int(sys.argv[2])
if kind == "signed":
    proof = {"signedBlockProof": {"blockSignature": base64.b64encode(bytes(size)).decode()}}
else:
    proof = {"blockStateProof": {"paths": [{"hash": base64.b64encode(bytes(48)).decode()}]}}
print(json.dumps({"status": "SUCCESS", "block": {"items": [{"blockProof": proof}]}}, indent=2))
PY
