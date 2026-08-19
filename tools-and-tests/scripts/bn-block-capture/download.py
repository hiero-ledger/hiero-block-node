"""Bulk-download blocks from a Block Node and store as ``<n>.blk.zstd``.

Each output file is ``zstd(Block.SerializeToString())`` — the standard
Block Node on-disk format.
"""
import argparse
import concurrent.futures
import os
import sys
import threading
import time
from typing import Iterable

import zstandard

from bn_client import BlockNodeClient, FetchError, NotAvailableError


class BulkDownloader:
    def __init__(self, endpoint: str, output_dir: str, level: int = 3, request_timeout: float = 120.0) -> None:
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        self.level = level
        self.client = BlockNodeClient(endpoint, request_timeout=request_timeout)
        self._lock = threading.Lock()
        self._done = 0
        self._failed = []
        self._skipped = 0
        self._bytes_written = 0

    def _save_one(self, block_number: int) -> str:
        out_path = os.path.join(self.output_dir, f"{block_number:010d}.blk.zstd")
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            with self._lock:
                self._skipped += 1
                self._done += 1
            return "SKIP"
        try:
            _, raw = self.client.get_block(block_number, retries=5)
        except NotAvailableError:
            with self._lock:
                self._failed.append((block_number, "NOT_AVAILABLE"))
                self._done += 1
            return "NOT_AVAILABLE"
        except FetchError as exc:
            with self._lock:
                self._failed.append((block_number, str(exc)))
                self._done += 1
            return f"ERROR:{exc}"

        compressed = zstandard.ZstdCompressor(level=self.level).compress(raw)
        tmp = out_path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(compressed)
        os.replace(tmp, out_path)
        with self._lock:
            self._done += 1
            self._bytes_written += len(compressed)
        return "OK"

    def run(self, block_numbers: Iterable[int], parallel: int = 16) -> dict:
        targets = list(block_numbers)
        total = len(targets)
        start = time.time()
        print(f"  Downloading {total} blocks -> {self.output_dir} (parallel={parallel})", file=sys.stderr)
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = {pool.submit(self._save_one, n): n for n in targets}
            last_print = time.time()
            for fut in concurrent.futures.as_completed(futures):
                try:
                    fut.result()
                except Exception as exc:
                    with self._lock:
                        self._failed.append((futures[fut], f"EXC:{exc}"))
                if time.time() - last_print > 5.0:
                    with self._lock:
                        d, b, sk = self._done, self._bytes_written, self._skipped
                    elapsed = time.time() - start
                    rate = d / elapsed if elapsed > 0 else 0
                    mb = b / (1024 * 1024)
                    print(
                        f"    [{d}/{total}] {rate:.1f} blocks/s, {mb:.1f} MB written, {sk} skipped",
                        file=sys.stderr,
                    )
                    last_print = time.time()

        elapsed = time.time() - start
        with self._lock:
            failed = list(self._failed)
            bytes_written = self._bytes_written
            skipped = self._skipped
        ok = total - len(failed) - skipped
        print(
            f"  Done: {ok} fetched, {skipped} skipped, {len(failed)} failed in {elapsed:.0f}s, {bytes_written/(1024*1024):.1f} MB written",
            file=sys.stderr,
        )
        if failed:
            print(f"  First 5 failures: {failed[:5]}", file=sys.stderr)
        return {"ok": ok, "skipped": skipped, "failed": failed, "bytes_written": bytes_written}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True, help="Block Node gRPC endpoint, e.g. host:port")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--parallel", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    dl = BulkDownloader(args.endpoint, args.out, request_timeout=args.timeout)
    result = dl.run(range(args.start, args.start + args.count), parallel=args.parallel)
    if result["failed"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
