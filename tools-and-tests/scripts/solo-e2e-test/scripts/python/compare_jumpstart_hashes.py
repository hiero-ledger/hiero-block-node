#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Jumpstart Merkle Hash Comparison Tool

Extracts and compares block hashes from jumpstart.bin files to validate
that WRB CLI wrapping produces identical cryptographic state.

Usage:
    python3 compare_jumpstart_hashes.py <jumpstart1.bin> <jumpstart2.bin>

Example:
    python3 compare_jumpstart_hashes.py \
        /tmp/wrb-cli-phase2-validation/cli-wrapped-blocks/jumpstart.bin \
        /tmp/bn-blocks/jumpstart.bin
"""

import struct
import sys
from pathlib import Path
from typing import Dict, Any


class JumpstartFile:
    """Represents a jumpstart.bin file with parsed contents."""

    def __init__(self, file_path: Path):
        """
        Parse jumpstart.bin file.

        Args:
            file_path: Path to jumpstart.bin file

        Format:
            - Block number (8 bytes, big-endian long)
            - Block hash (48 bytes - SHA-384)
            - Consensus timestamp hash (48 bytes - SHA-384)
            - Output items tree root hash (48 bytes - SHA-384)
            - Streaming hasher leaf count (8 bytes, big-endian long)
            - Hash count (4 bytes, big-endian int)
            - Pending subtree hashes (48 bytes each)
        """
        self.file_path = file_path

        with open(file_path, 'rb') as f:
            # Read block number
            self.block_number = struct.unpack('>Q', f.read(8))[0]

            # Read hashes (48 bytes each - SHA-384)
            self.block_hash = f.read(48).hex()
            self.consensus_timestamp_hash = f.read(48).hex()
            self.output_items_tree_root_hash = f.read(48).hex()

            # Read streaming hasher state
            self.leaf_count = struct.unpack('>Q', f.read(8))[0]
            self.hash_count = struct.unpack('>I', f.read(4))[0]

            # Read pending subtree hashes
            self.pending_hashes = []
            for i in range(self.hash_count):
                hash_bytes = f.read(48)
                if len(hash_bytes) == 48:
                    self.pending_hashes.append(hash_bytes.hex())
                else:
                    print(f"WARNING: Expected 48 bytes for hash {i}, got {len(hash_bytes)}", file=sys.stderr)
                    break

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for comparison."""
        return {
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "consensus_timestamp_hash": self.consensus_timestamp_hash,
            "output_items_tree_root_hash": self.output_items_tree_root_hash,
            "streaming_hasher": {
                "leaf_count": self.leaf_count,
                "hash_count": self.hash_count,
                "pending_hashes": self.pending_hashes
            }
        }

    def __str__(self) -> str:
        """String representation."""
        lines = [
            f"Jumpstart File: {self.file_path}",
            f"  Block Number: {self.block_number}",
            f"  Block Hash: {self.block_hash}",
            f"  Consensus Timestamp Hash: {self.consensus_timestamp_hash}",
            f"  Output Items Tree Root Hash: {self.output_items_tree_root_hash}",
            f"  Streaming Hasher:",
            f"    Leaf Count: {self.leaf_count}",
            f"    Hash Count: {self.hash_count}",
        ]

        if self.pending_hashes:
            lines.append("    Pending Hashes:")
            for i, hash_hex in enumerate(self.pending_hashes):
                lines.append(f"      [{i}]: {hash_hex}")

        return "\n".join(lines)


def compare_jumpstart_files(file1: Path, file2: Path) -> bool:
    """
    Compare two jumpstart.bin files for cryptographic equivalence.

    Args:
        file1: Path to first jumpstart.bin
        file2: Path to second jumpstart.bin

    Returns:
        True if files are cryptographically identical, False otherwise
    """
    print(f"Comparing jumpstart files:")
    print(f"  File 1: {file1}")
    print(f"  File 2: {file2}")
    print()

    # Parse both files
    try:
        js1 = JumpstartFile(file1)
        print("File 1 contents:")
        print(js1)
        print()
    except Exception as e:
        print(f"ERROR: Failed to parse {file1}: {e}", file=sys.stderr)
        return False

    try:
        js2 = JumpstartFile(file2)
        print("File 2 contents:")
        print(js2)
        print()
    except Exception as e:
        print(f"ERROR: Failed to parse {file2}: {e}", file=sys.stderr)
        return False

    # Compare fields
    all_match = True
    differences = []

    if js1.block_number != js2.block_number:
        all_match = False
        differences.append(f"Block number: {js1.block_number} vs {js2.block_number}")

    if js1.block_hash != js2.block_hash:
        all_match = False
        differences.append(f"Block hash: {js1.block_hash[:16]}... vs {js2.block_hash[:16]}...")

    if js1.consensus_timestamp_hash != js2.consensus_timestamp_hash:
        all_match = False
        differences.append(f"Consensus timestamp hash: {js1.consensus_timestamp_hash[:16]}... vs {js2.consensus_timestamp_hash[:16]}...")

    if js1.output_items_tree_root_hash != js2.output_items_tree_root_hash:
        all_match = False
        differences.append(f"Output items tree root hash: {js1.output_items_tree_root_hash[:16]}... vs {js2.output_items_tree_root_hash[:16]}...")

    if js1.leaf_count != js2.leaf_count:
        all_match = False
        differences.append(f"Streaming hasher leaf count: {js1.leaf_count} vs {js2.leaf_count}")

    if js1.hash_count != js2.hash_count:
        all_match = False
        differences.append(f"Streaming hasher hash count: {js1.hash_count} vs {js2.hash_count}")

    if js1.pending_hashes != js2.pending_hashes:
        all_match = False
        differences.append(f"Pending subtree hashes differ ({len(js1.pending_hashes)} vs {len(js2.pending_hashes)} hashes)")

        # Show first difference
        for i, (h1, h2) in enumerate(zip(js1.pending_hashes, js2.pending_hashes)):
            if h1 != h2:
                differences.append(f"  First difference at index {i}: {h1[:16]}... vs {h2[:16]}...")
                break

    # Print results
    print("=" * 60)
    print("COMPARISON RESULTS")
    print("=" * 60)

    if all_match:
        print("✅ SUCCESS: Jumpstart files are CRYPTOGRAPHICALLY IDENTICAL")
        print()
        print("All fields match:")
        print(f"  ✓ Block number: {js1.block_number}")
        print(f"  ✓ Block hash: {js1.block_hash}")
        print(f"  ✓ Consensus timestamp hash: {js1.consensus_timestamp_hash}")
        print(f"  ✓ Output items tree root hash: {js1.output_items_tree_root_hash}")
        print(f"  ✓ Streaming hasher leaf count: {js1.leaf_count}")
        print(f"  ✓ Streaming hasher hash count: {js1.hash_count}")
        print(f"  ✓ All {js1.hash_count} pending subtree hashes match")
        return True
    else:
        print("❌ FAILURE: Jumpstart files DIFFER")
        print()
        print(f"Found {len(differences)} difference(s):")
        for diff in differences:
            print(f"  ✗ {diff}")
        return False


def main():
    if len(sys.argv) != 3:
        print("Usage: compare_jumpstart_hashes.py <jumpstart1.bin> <jumpstart2.bin>", file=sys.stderr)
        print()
        print("Compares two jumpstart.bin files for cryptographic equivalence.")
        print("Returns exit code 0 if identical, 1 if different or error.")
        sys.exit(1)

    file1 = Path(sys.argv[1])
    file2 = Path(sys.argv[2])

    # Validate files exist
    if not file1.exists():
        print(f"ERROR: File not found: {file1}", file=sys.stderr)
        sys.exit(1)

    if not file2.exists():
        print(f"ERROR: File not found: {file2}", file=sys.stderr)
        sys.exit(1)

    # Compare
    if compare_jumpstart_files(file1, file2):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()