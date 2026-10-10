"""TPS profiler / Schnorr->WRAPS transition finder for a Block Node.

Usage:
  python profiler.py --endpoint HOST:PORT scan       --start N --end M --count K --out file.csv
  python profiler.py --endpoint HOST:PORT transition --start 300 --end 500
  python profiler.py --endpoint HOST:PORT drill      --center N --window 200 --out file.csv

CSV columns: block_number, tps, signed_tx_count, item_count, sig_bytes, raw_bytes, ts_seconds, ts_nanos, status
"""
import argparse
import concurrent.futures
import csv
import os
import sys
import time

from bn_client import BlockNodeClient, FetchError, NotAvailableError

SCHNORR_LO, SCHNORR_HI = 2900, 2940
WRAPS_LO, WRAPS_HI = 3420, 3460


def probe_block(client: BlockNodeClient, block_number: int) -> dict:
    try:
        block, raw = client.get_block(block_number, retries=3)
    except NotAvailableError:
        return {"block_number": block_number, "status": "NOT_AVAILABLE"}
    except FetchError as exc:
        return {"block_number": block_number, "status": f"ERROR:{exc}"}
    metrics = client.estimate_block_metrics(block)
    sig_len = client.proof_signature_length(block)
    return {
        "block_number": block_number,
        "status": "OK",
        "ts_seconds": metrics["timestamp_seconds"],
        "ts_nanos": metrics["timestamp_nanos"],
        "signed_tx_count": metrics["signed_tx_count"],
        "item_count": metrics["item_count"],
        "sig_bytes": sig_len,
        "raw_bytes": len(raw),
    }


def probe_many(endpoint: str, block_numbers, parallel: int = 16) -> list:
    """Probe a list of block numbers concurrently and return rows in input order."""
    client = BlockNodeClient(endpoint)
    results = [None] * len(block_numbers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as pool:
        future_to_idx = {
            pool.submit(probe_block, client, n): i for i, n in enumerate(block_numbers)
        }
        done = 0
        total = len(block_numbers)
        for fut in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = {
                    "block_number": block_numbers[idx],
                    "status": f"ERROR:{exc}",
                }
            done += 1
            if done % 10 == 0 or done == total:
                print(f"  [{done}/{total}]", file=sys.stderr)
    return results


def compute_tps(rows: list) -> list:
    """Annotate rows with tps (dt to next sample). Rows must be sorted by block_number."""
    rows_sorted = sorted([r for r in rows if r.get("status") == "OK"], key=lambda r: r["block_number"])
    by_num = {r["block_number"]: r for r in rows_sorted}
    for i, row in enumerate(rows_sorted):
        row["tps"] = None
        if i + 1 < len(rows_sorted):
            nxt = rows_sorted[i + 1]
            block_gap = nxt["block_number"] - row["block_number"]
            dt = (nxt["ts_seconds"] - row["ts_seconds"]) + (
                nxt["ts_nanos"] - row["ts_nanos"]
            ) / 1e9
            # tps within THIS block: signed_tx_count / per-block duration (= dt / block_gap)
            if dt > 0 and block_gap > 0:
                per_block_seconds = dt / block_gap
                row["tps"] = round(row["signed_tx_count"] / per_block_seconds, 2)
    return rows_sorted


def write_csv(rows: list, path: str) -> None:
    if not rows:
        print("No rows to write", file=sys.stderr)
        return
    fields = ["block_number", "tps", "signed_tx_count", "item_count", "sig_bytes", "raw_bytes", "ts_seconds", "ts_nanos", "status"]
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def cmd_scan(args: argparse.Namespace) -> None:
    start, end, count = args.start, args.end, args.count
    if count <= 1:
        block_nums = [start]
    else:
        step = max((end - start) // (count - 1), 1)
        block_nums = list(range(start, end + 1, step))[:count]
    print(f"Probing {len(block_nums)} blocks across [{start},{end}] (step={block_nums[1] - block_nums[0] if len(block_nums) > 1 else '-'})", file=sys.stderr)
    raw_rows = probe_many(args.endpoint, block_nums, parallel=args.parallel)
    # For accurate TPS we need to also fetch each sample's IMMEDIATE next block,
    # so we can divide tx_count by actual per-block duration.
    needed_neighbors = [r["block_number"] + 1 for r in raw_rows if r.get("status") == "OK"]
    neighbor_rows = probe_many(args.endpoint, needed_neighbors, parallel=args.parallel)
    combined = raw_rows + neighbor_rows
    annotated = compute_tps(combined)
    # Filter back to ONLY the originally-requested block numbers
    requested = set(block_nums)
    final = [r for r in annotated if r["block_number"] in requested]
    write_csv(final, args.out)
    print(f"Wrote {len(final)} rows to {args.out}", file=sys.stderr)


def cmd_drill(args: argparse.Namespace) -> None:
    """Probe a dense window of consecutive blocks around ``--center``."""
    half = args.window // 2
    lo = max(args.center - half, 0)
    hi = args.center + half
    block_nums = list(range(lo, hi + 1))
    print(f"Drilling {len(block_nums)} consecutive blocks [{lo},{hi}]", file=sys.stderr)
    rows = probe_many(args.endpoint, block_nums, parallel=args.parallel)
    annotated = compute_tps(rows)
    write_csv(annotated, args.out)
    print(f"Wrote {len(annotated)} rows to {args.out}", file=sys.stderr)


def cmd_transition(args: argparse.Namespace) -> None:
    """Find the Schnorr->WRAPS proof transition in [start, end]."""
    client = BlockNodeClient(args.endpoint)
    start, end = args.start, args.end

    def sig_kind(sig_len: int) -> str:
        if SCHNORR_LO <= sig_len <= SCHNORR_HI:
            return "SCHNORR"
        if WRAPS_LO <= sig_len <= WRAPS_HI:
            return "WRAPS"
        if sig_len == 0:
            return "EMPTY"
        return f"UNKNOWN({sig_len})"

    print(f"Scanning sigs in [{start},{end}] for Schnorr->WRAPS transition", file=sys.stderr)
    # Sparse scan every 10 blocks to find the WRAPS side
    nums = list(range(start, end + 1, 10))
    rows = probe_many(args.endpoint, nums, parallel=args.parallel)
    last_schnorr = None
    first_wraps = None
    for row in sorted([r for r in rows if r.get("status") == "OK"], key=lambda r: r["block_number"]):
        kind = sig_kind(row["sig_bytes"])
        print(f"  block {row['block_number']}: sig={row['sig_bytes']} -> {kind}", file=sys.stderr)
        if kind == "SCHNORR":
            last_schnorr = row["block_number"]
        elif kind == "WRAPS" and first_wraps is None:
            first_wraps = row["block_number"]

    if first_wraps is None:
        # Either entire range is Schnorr, or entire range is WRAPS, or neither
        if last_schnorr is None:
            print("No SCHNORR or WRAPS blocks seen — proof sig sizes unrecognized.", file=sys.stderr)
            sys.exit(1)
        print(f"All Schnorr in [{start},{end}], transition not yet seen.", file=sys.stderr)
        sys.exit(1)
    if last_schnorr is None:
        # WRAPS from the start of range
        print(f"All WRAPS in [{start},{end}]; transition before {start}.", file=sys.stderr)
        # Binary search downward from start
        lo, hi = 0, start
    else:
        lo, hi = last_schnorr, first_wraps

    # Binary search for exact transition
    while hi - lo > 1:
        mid = (lo + hi) // 2
        block, _ = client.get_block(mid, retries=5)
        sig = client.proof_signature_length(block)
        kind = sig_kind(sig)
        print(f"    bsearch mid={mid}: sig={sig} -> {kind}", file=sys.stderr)
        if kind == "SCHNORR":
            lo = mid
        else:
            hi = mid

    print(f"\nFirst WRAPS block: {hi}", file=sys.stderr)
    print(f"Last  SCHNORR  block: {lo}", file=sys.stderr)
    print(f"TRANSITION_BLOCK={hi}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True, help="Block Node gRPC endpoint, e.g. host:port")
    sub = parser.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", help="sparse TPS sample across a range")
    s.add_argument("--start", type=int, required=True)
    s.add_argument("--end", type=int, required=True)
    s.add_argument("--count", type=int, default=150)
    s.add_argument("--parallel", type=int, default=16)
    s.add_argument("--out", required=True)
    s.set_defaults(func=cmd_scan)

    d = sub.add_parser("drill", help="dense scan around a center block")
    d.add_argument("--center", type=int, required=True)
    d.add_argument("--window", type=int, default=200)
    d.add_argument("--parallel", type=int, default=16)
    d.add_argument("--out", required=True)
    d.set_defaults(func=cmd_drill)

    t = sub.add_parser("transition", help="find Schnorr->WRAPS transition")
    t.add_argument("--start", type=int, default=300)
    t.add_argument("--end", type=int, default=500)
    t.add_argument("--parallel", type=int, default=8)
    t.set_defaults(func=cmd_transition)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
