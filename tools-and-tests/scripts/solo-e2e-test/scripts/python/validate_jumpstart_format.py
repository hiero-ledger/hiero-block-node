#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate jumpstart.bin file format and contents."""

import struct
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print("Usage: validate_jumpstart_format.py <jumpstart_file>", file=sys.stderr)
        sys.exit(1)

    jumpstart_file = Path(sys.argv[1])

    with open(jumpstart_file, 'rb') as f:
        # Read block number (8 bytes, big-endian long)
        block_number = struct.unpack('>Q', f.read(8))[0]

        # Read block hash (48 bytes - SHA-384)
        block_hash = f.read(48).hex()

        # Read consensus timestamp hash (48 bytes - SHA-384)
        consensus_timestamp_hash = f.read(48).hex()

        # Read output items tree root hash (48 bytes - SHA-384)
        output_items_tree_root_hash = f.read(48).hex()

        # Read streaming hasher leaf count (8 bytes, big-endian long)
        leaf_count = struct.unpack('>Q', f.read(8))[0]

        # Read hash count (4 bytes, big-endian int)
        hash_count = struct.unpack('>I', f.read(4))[0]

        # Read pending subtree hashes (48 bytes each)
        pending_hashes = []
        for i in range(hash_count):
            hash_bytes = f.read(48)
            if len(hash_bytes) == 48:
                pending_hashes.append(hash_bytes.hex())
            else:
                print(f"WARNING: Expected 48 bytes for hash {i}, got {len(hash_bytes)}", file=sys.stderr)
                break

        # Display results
        print(f"Block Number: {block_number}", file=sys.stderr)
        print(f"Block Hash: {block_hash}", file=sys.stderr)
        print(f"Consensus Timestamp Hash: {consensus_timestamp_hash}", file=sys.stderr)
        print(f"Output Items Tree Root Hash: {output_items_tree_root_hash}", file=sys.stderr)
        print(f"Streaming Hasher Leaf Count: {leaf_count}", file=sys.stderr)
        print(f"Pending Subtree Hashes Count: {hash_count}", file=sys.stderr)

        for i, hash_hex in enumerate(pending_hashes):
            print(f"  Hash {i}: {hash_hex}", file=sys.stderr)

        # Verify leaf count and hash count are reasonable
        if block_number < 0:
            print(f"ERROR: Invalid block number: {block_number}", file=sys.stderr)
            sys.exit(1)

        if leaf_count < 0:
            print(f"ERROR: Invalid leaf count: {leaf_count}", file=sys.stderr)
            sys.exit(1)

        if hash_count < 0 or hash_count > 64:  # O(log n), max ~64 for 2^64 blocks
            print(f"ERROR: Invalid hash count: {hash_count}", file=sys.stderr)
            sys.exit(1)

        if len(pending_hashes) != hash_count:
            print(f"ERROR: Hash count mismatch: expected {hash_count}, got {len(pending_hashes)}", file=sys.stderr)
            sys.exit(1)

        print(f"Jumpstart file validation PASSED", file=sys.stderr)

if __name__ == '__main__':
    main()
