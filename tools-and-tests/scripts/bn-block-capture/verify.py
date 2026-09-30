"""Verify a downloaded block bucket.

For each ``<n>.blk.zstd`` file:
  - confirm non-empty,
  - decompress and parse Block,
  - record signed_transaction count + block_timestamp.

Then compute actual TPS over the bucket as: total_txs / (last_ts - first_ts).
"""
import argparse
import os
import sys

import zstandard

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "proto-py"))
from block.stream import block_pb2

from bn_client import BlockNodeClient


def verify_bucket(directory: str) -> dict:
    files = sorted(f for f in os.listdir(directory) if f.endswith(".blk.zstd"))
    if not files:
        return {"directory": directory, "count": 0, "error": "no files"}

    decomp = zstandard.ZstdDecompressor()
    total_txs = 0
    first_ts = None
    last_ts = None
    first_ts_nanos = 0
    last_ts_nanos = 0
    first_block = None
    last_block = None
    total_raw = 0
    total_compressed = 0
    bad = []

    for name in files:
        path = os.path.join(directory, name)
        size = os.path.getsize(path)
        if size == 0:
            bad.append((name, "empty"))
            continue
        with open(path, "rb") as fh:
            compressed = fh.read()
        try:
            raw = decomp.decompress(compressed, max_output_size=512 * 1024 * 1024)
        except Exception as exc:
            bad.append((name, f"zstd: {exc}"))
            continue
        try:
            block = block_pb2.Block.FromString(raw)
        except Exception as exc:
            bad.append((name, f"proto: {exc}"))
            continue
        metrics = BlockNodeClient.estimate_block_metrics(block)
        total_txs += metrics["signed_tx_count"]
        total_raw += len(raw)
        total_compressed += size
        ts = metrics["timestamp_seconds"] + metrics["timestamp_nanos"] / 1e9
        block_number = int(name[: -len(".blk.zstd")])
        if first_ts is None or ts < first_ts:
            first_ts = ts
            first_block = block_number
        if last_ts is None or ts > last_ts:
            last_ts = ts
            last_block = block_number

    duration = (last_ts - first_ts) if first_ts is not None else 0
    tps = total_txs / duration if duration > 0 else 0
    return {
        "directory": directory,
        "count": len(files),
        "bad": bad,
        "total_txs": total_txs,
        "first_block": first_block,
        "last_block": last_block,
        "duration_seconds": round(duration, 2),
        "avg_tps": round(tps, 2),
        "total_raw_mb": round(total_raw / (1024 * 1024), 1),
        "total_compressed_mb": round(total_compressed / (1024 * 1024), 1),
        "compression_ratio": round(total_raw / total_compressed, 2) if total_compressed > 0 else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dirs", nargs="+", help="Bucket directories to verify")
    args = parser.parse_args()

    for d in args.dirs:
        result = verify_bucket(d)
        print(f"=== {d} ===")
        for k, v in result.items():
            if k == "bad" and not v:
                continue
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
