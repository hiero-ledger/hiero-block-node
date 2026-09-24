"""Block Node client backed by ``grpcurl`` (Python grpcio is incompatible with
this Helidon-based BN — it rejects the HTTP/2 SETTINGS frame with GOAWAY).

The grpcurl subprocess fetches JSON which we re-parse through the generated
protobuf classes and re-serialize to binary, so the on-disk file is bit-exact
to a normally-stored Block message.
"""
import json
import os
import subprocess
import sys
import time
from typing import Optional, Tuple

PROTO_PY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proto-py")
if PROTO_PY not in sys.path:
    sys.path.insert(0, PROTO_PY)

from google.protobuf import json_format

from block_node.api import block_access_service_pb2, node_service_pb2
from block.stream import block_pb2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
API_PROTO_DIR = os.path.join(REPO_ROOT, "protobuf-sources", "src", "main", "proto")
STREAM_PROTO_DIR = os.path.join(REPO_ROOT, "protobuf-sources", "block-node-protobuf")

MAX_MSG_SIZE = 300 * 1024 * 1024


class FetchError(Exception):
    """Raised when a block fetch fails after retries."""


class NotAvailableError(FetchError):
    """Block is genuinely not available (NOT_FOUND / NOT_AVAILABLE)."""


def _run_grpcurl(
    endpoint: str, proto_file: str, method: str, payload: str, timeout: float = 30.0
) -> str:
    cmd = [
        "grpcurl",
        "-plaintext",
        "-import-path",
        API_PROTO_DIR,
        "-import-path",
        STREAM_PROTO_DIR,
        "-proto",
        proto_file,
        "-max-msg-sz",
        str(MAX_MSG_SIZE),
        "-d",
        payload,
        endpoint,
        method,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise FetchError(f"grpcurl failed (rc={proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout


class BlockNodeClient:
    def __init__(self, endpoint: str, request_timeout: float = 30.0) -> None:
        for prefix in ("tcp://", "http://", "https://"):
            if endpoint.startswith(prefix):
                endpoint = endpoint[len(prefix):]
        self.endpoint = endpoint.rstrip("/")
        self.request_timeout = request_timeout

    def server_status(self) -> dict:
        out = _run_grpcurl(
            self.endpoint,
            "block-node/api/node_service.proto",
            "org.hiero.block.api.BlockNodeService/serverStatus",
            "{}",
            timeout=self.request_timeout,
        )
        msg = node_service_pb2.ServerStatusResponse()
        json_format.Parse(out, msg, ignore_unknown_fields=True)
        return {
            "first_available_block": int(getattr(msg, "first_available_block", 0)),
            "last_available_block": int(getattr(msg, "last_available_block", 0)),
        }

    def get_block(
        self, block_number: int, retries: int = 5, base_delay: float = 0.5
    ) -> Tuple[block_pb2.Block, bytes]:
        """Return ``(Block message, raw bytes)`` for ``block_number``.

        Raises ``NotAvailableError`` if the BN says the block isn't there;
        raises ``FetchError`` for hard failures after ``retries`` retries.
        """
        attempt = 0
        delay = base_delay
        while True:
            try:
                out = _run_grpcurl(
                    self.endpoint,
                    "block-node/api/block_access_service.proto",
                    "org.hiero.block.api.BlockAccessService/getBlock",
                    json.dumps({"block_number": block_number}),
                    timeout=self.request_timeout,
                )
            except FetchError as exc:
                if attempt >= retries:
                    raise
                attempt += 1
                time.sleep(min(delay, 8.0))
                delay *= 2
                continue

            # grpcurl might emit nothing if server hangs up — guard against it.
            stripped = out.strip()
            if not stripped:
                if attempt >= retries:
                    raise FetchError(f"empty response for block {block_number}")
                attempt += 1
                time.sleep(min(delay, 8.0))
                delay *= 2
                continue

            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                if attempt >= retries:
                    raise FetchError(f"invalid JSON for block {block_number}: {exc}") from exc
                attempt += 1
                time.sleep(min(delay, 8.0))
                delay *= 2
                continue

            status = payload.get("status", "SUCCESS")
            if status in ("NOT_FOUND", "NOT_AVAILABLE"):
                raise NotAvailableError(f"block {block_number}: {status}")
            if status not in ("SUCCESS", 1, "1"):
                raise FetchError(f"block {block_number}: status={status}")

            resp = block_access_service_pb2.BlockResponse()
            json_format.Parse(stripped, resp, ignore_unknown_fields=True)
            block = resp.block
            raw = block.SerializeToString()
            return block, raw

    @staticmethod
    def estimate_block_metrics(block: block_pb2.Block) -> dict:
        signed_tx_count = 0
        item_count = 0
        timestamp_seconds = 0
        timestamp_nanos = 0
        for item in block.items:
            item_count += 1
            which = item.WhichOneof("item")
            if which == "signed_transaction":
                signed_tx_count += 1
            elif which == "block_header" and item.block_header.HasField("block_timestamp"):
                timestamp_seconds = int(item.block_header.block_timestamp.seconds)
                timestamp_nanos = int(item.block_header.block_timestamp.nanos)
        return {
            "signed_tx_count": signed_tx_count,
            "item_count": item_count,
            "timestamp_seconds": timestamp_seconds,
            "timestamp_nanos": timestamp_nanos,
        }

    @staticmethod
    def proof_signature_length(block: block_pb2.Block) -> int:
        for item in block.items:
            if item.WhichOneof("item") == "block_proof":
                proof = item.block_proof
                if proof.HasField("signed_block_proof"):
                    return len(proof.signed_block_proof.block_signature)
                return 0
        return 0
