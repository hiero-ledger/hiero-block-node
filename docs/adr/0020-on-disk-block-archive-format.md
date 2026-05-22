# 0005 - Block Node On-Disk Block Archive Format

Date: 2024-10-11

## Status

Accepted

## Context

Block Nodes must store potentially hundreds of terabytes of block data on local HDD. The archive format must balance three competing concerns:

1. **Size on disk** — compression is essential; even a small percentage reduction translates to many TB at scale.
2. **Read performance** — streaming clients require blocks to be decompressed quickly; slow decompression limits concurrent streaming capacity.
3. **File system scalability** — billions of blocks as individual files creates inode exhaustion and metadata overhead problems on standard file systems.

Formats evaluated: Gzip, Zstd, LZ4 for compression; TAR, ZIP (uncompressed container), custom binary for packaging.

## Decision

Blocks are stored as **Zstd-compressed individual block files packed into uncompressed ZIP archives of 1,000 blocks each**.

- **Compression**: Zstd. Benchmarks show ~20% better compression ratio than Gzip/ZIP and ~3.5× faster decompression. Java support via the `zstd-jni` library (BSD license).
- **Container**: Uncompressed ZIP. Well-supported by Java standard libraries; ZIP random-access allows individual block retrieval without decompressing the entire archive.
- **Batch size**: 1,000 blocks per ZIP file. Sample data produces ZIP files in the 40 MB–1.4 GB range.
- **Tooling**: Format is composed of standard `zip` and `zstd` CLI tools, enabling manual inspection and recovery without custom tooling.
- **Uncompressed size**: Stored in `ZipEntry` extra fields using a custom 16-bit key to support non-streaming decompression use cases.

This is an internal Block Node storage format and is not exposed to consumers directly.

## Consequences

- The format is not locked in — as it is internal to BN, it can be changed in a future release with a migration tool if benchmarks reveal a better option.
- The `zstd-jni` JNI library adds a native dependency; cross-platform packaging (Linux aarch64, x86_64) must be validated.
- Future consideration: Intel ISA-L / IPP as a high-performance drop-in Zlib replacement for gzip used in network compression paths (separate from storage).
- ZIP file sizes vary significantly based on block content; operators should provision storage with the upper bound (~1.4 GB per 1,000 blocks at high TPS) in mind.
- Configurable storage backends (e.g. object storage, alternative compression) may be offered in future releases.
