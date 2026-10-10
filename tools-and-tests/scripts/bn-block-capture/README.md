# bn-block-capture

Python tooling to capture block recordings from a Hiero Block Node and store them as standard `<n>.blk.zstd` files (binary `Block.SerializeToString()` zstd-compressed — bit-exact to what BNs write to disk).

Designed for sampling blocks at known TPS plateaus (idle, 100, 1.5k, 7k, 10k, ...) and around the Schnorr -> WRAPS TSS signature transition.

## Why grpcurl, not native Python gRPC

Helidon-based Block Nodes reject Python `grpcio`'s HTTP/2 settings frame with `GOAWAY error code 1` immediately after the SETTINGS exchange. `grpcurl` (Go-based) works fine, so `bn_client.py` shells out to `grpcurl`, parses the JSON response, and re-serializes through the generated protobuf classes. The on-disk bytes are still identical to a native binary fetch.

## Prerequisites

- `python3` (>=3.8) with `venv`
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) on PATH (`brew install grpcurl`)
- Run from a checkout of the Hiero Block Node repo (the script uses `../../../protobuf-sources/`)

## Setup (one-time)

```
cd tools-and-tests/scripts/bn-block-capture
./setup.sh
```

This creates `.venv/`, installs Python deps, and compiles all `.proto` files into `proto-py/`. Re-run after proto changes.

## Scripts

|     Script     |                                              Purpose                                              |
|----------------|---------------------------------------------------------------------------------------------------|
| `bn_client.py` | gRPC client wrapping `grpcurl`. Exposes `BlockNodeClient.server_status()` / `get_block()`.        |
| `profiler.py`  | Sparse + dense TPS scanner, plus binary-search Schnorr->WRAPS transition finder.                  |
| `download.py`  | Bulk downloader. Parallel workers, zstd compression, resumable (skips existing files).            |
| `verify.py`    | Decompresses every file in a bucket, parses the Block, recomputes avg TPS over the actual window. |

All scripts take `--endpoint host:port`.

## Workflow

### 1. Survey the BN

```
.venv/bin/python profiler.py --endpoint host:port scan \
  --start 0 --end 778000 --count 150 --parallel 16 --out scan-phase1.csv
```

CSV gives you the TPS profile: each row is one sampled block with its measured TPS (computed from this block's tx count divided by `ts(N+1) - ts(N)`). Open it and identify plateaus (consecutive samples with similar TPS).

### 2. Find the Schnorr -> WRAPS transition

The first WRAPS block typically lives in `[~300, ~1000]` for current BNCE-style networks.

```
.venv/bin/python profiler.py --endpoint host:port transition --start 100 --end 1500
```

Prints `TRANSITION_BLOCK=N` (the first WRAPS block). Note that this block can be ~95 MB raw because it carries the TSS WRAPS-key payload.

### 3. Drill into plateau boundaries (optional)

If a plateau's exact start/end matters, scan it densely:

```
.venv/bin/python profiler.py --endpoint host:port scan \
  --start 460000 --end 530000 --count 35 --parallel 16 --out drill-1500-7k.csv
```

### 4. Download the buckets

Pick centers safely inside each plateau and a window size. The downloader writes `<n:010>.blk.zstd` into the output dir.

Default window is 100 blocks per bucket (configurable via `--count`). Centers below are illustrative — re-derive them from your own scan, since block ranges grow as the network advances.

```
.venv/bin/python download.py --endpoint host:port --out tps-100 \
  --start 349500 --count 100 --parallel 20 --timeout 60

.venv/bin/python download.py --endpoint host:port --out tps-1500 \
  --start 479500 --count 100 --parallel 16 --timeout 120

.venv/bin/python download.py --endpoint host:port --out tps-7000 \
  --start 513500 --count 100 --parallel 12 --timeout 180

.venv/bin/python download.py --endpoint host:port --out tps-10000 \
  --start 677000 --count 100 --parallel 8 --timeout 240
```

Tune `--parallel` per bucket: small blocks (low TPS) tolerate ~20+ workers; 10k-TPS blocks are 20-30 MB each so 6-10 workers is more honest. Transient `NOT_AVAILABLE` failures are normal; re-run the same command — it skips existing files and only retries the misses.

### 5. Verify

```
.venv/bin/python verify.py tps-100 tps-1500 tps-7000 tps-10000
```

Reports per-bucket: file count, total transactions, first/last block, duration, **measured avg TPS**, raw vs compressed size.

## Output Format

Each `<n>.blk.zstd` is `zstd(Block.SerializeToString())`. Decompress + use existing block tools:

```
zstd -d 0000349500.blk.zstd -o 0000349500.blk
./tool.sh blocks json <dir>
./tool.sh blocks validate <dir>
```

## Typical Sizes (zstd level 3)

| Bucket TPS | Avg raw size | Avg zstd size | Compression |
|------------|--------------|---------------|-------------|
| 7 (idle)   | ~33 KB       | ~25 KB        | 1.3x        |
| 100        | ~200 KB      | ~100 KB       | 2.0x        |
| 1,500      | ~4.7 MB      | ~2.8 MB       | 1.7x        |
| 7,000      | ~20 MB       | ~12 MB        | 1.7x        |
| 10,000     | ~30 MB       | ~18 MB        | 1.7x        |

So the default 100 blocks at 10k TPS ≈ ~1.8 GB on disk (1,000 blocks would be ~18 GB).

## Throughput Notes

Per-block fetch time is dominated by JSON serialization on the BN + parse in Python (the grpcurl JSON round-trip). Empirically:
- ~14 blocks/s at 100 TPS (small blocks)
- ~1.6 blocks/s at 1.5k TPS
- ~0.2-0.3 blocks/s at 7-10k TPS

So 1,000 blocks at 10k TPS takes ~1.5h. For bigger captures consider switching to the streaming subscribe service (`subscribeBlockStream`), which keeps a single HTTP/2 connection alive and eliminates per-request startup cost. Not implemented here yet.

## Troubleshooting

- **`GOAWAY received; Error code: 1`** — you're hitting native `grpcio` instead of `grpcurl`. Make sure you're invoking the scripts in this folder, which always shell out to `grpcurl`.
- **`malformed header: missing HTTP content-type`** — server doesn't speak gRPC reflection. Use the explicit `-proto` flag (which the scripts do).
- **`NOT_AVAILABLE` on a few blocks in a 1,000-block run** — transient. Just re-run the same command; the downloader is resumable.
- **Block 361 (or whichever the WRAPS transition is) fails with timeout** — that block is ~95 MB. Bump `--timeout 240` and run with `--parallel 1`.
- **`platform' is not a package`** — `setup.sh` did not rename the generated `platform/` package to `_bn_platform/`. Re-run `./setup.sh`.

See `.claude/commands/bn-backup.md` for the higher-level workflow doc that wraps this README.
