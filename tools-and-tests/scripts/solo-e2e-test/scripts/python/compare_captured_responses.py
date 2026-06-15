#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Compare two directories of captured Mirror Node API responses.

Usage:
    python3 compare_captured_responses.py <dir1> <dir2> [--output report.json]

Example:
    python3 compare_captured_responses.py /tmp/mn1-responses /tmp/mn2-responses --output report.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


class OfflineComparator:
    """Compare captured API responses from two directories."""

    def __init__(self, dir1: Path, dir2: Path, verbose: bool = False):
        self.dir1 = dir1
        self.dir2 = dir2
        self.verbose = verbose
        self.differences = []

    def log(self, message: str):
        if self.verbose:
            print(f"[compare] {message}", file=sys.stderr)

    def load_json(self, file_path: Path) -> Any:
        """Load JSON file."""
        try:
            with open(file_path) as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading {file_path}: {e}", file=sys.stderr)
            return None

    def compare_files(self, filename: str) -> bool:
        """Compare same file from both directories."""
        file1 = self.dir1 / filename
        file2 = self.dir2 / filename

        if not file1.exists():
            self.differences.append(f"Missing in dir1: {filename}")
            return False
        if not file2.exists():
            self.differences.append(f"Missing in dir2: {filename}")
            return False

        data1 = self.load_json(file1)
        data2 = self.load_json(file2)

        if data1 is None or data2 is None:
            return False

        if data1 == data2:
            self.log(f"{filename}: ✓ Match")
            return True

        # Find differences
        diff_detail = self._deep_diff(data1, data2, filename)
        self.differences.append({
            "file": filename,
            "details": diff_detail
        })
        print(f"{filename}: ✗ Mismatch")
        return False

    def _deep_diff(self, obj1: Any, obj2: Any, path: str) -> str:
        """Deep comparison to identify specific differences."""
        if type(obj1) != type(obj2):
            return f"{path}: type mismatch ({type(obj1).__name__} vs {type(obj2).__name__})"

        if isinstance(obj1, dict):
            keys1 = set(obj1.keys())
            keys2 = set(obj2.keys())
            if keys1 != keys2:
                return f"{path}: key mismatch"

            for key in keys1:
                if obj1[key] != obj2[key]:
                    return self._deep_diff(obj1[key], obj2[key], f"{path}.{key}")

        elif isinstance(obj1, list):
            if len(obj1) != len(obj2):
                return f"{path}: length mismatch ({len(obj1)} vs {len(obj2)})"

            for i, (item1, item2) in enumerate(zip(obj1, obj2)):
                if item1 != item2:
                    return self._deep_diff(item1, item2, f"{path}[{i}]")

        else:
            if obj1 != obj2:
                return f"{path}: value mismatch"

        return f"{path}: unknown difference"

    def compare_all(self) -> bool:
        """Compare all captured files."""
        files_to_compare = [
            "network_nodes.json",
            "blocks.json",
            "transactions.json",
            "balances.json"
        ]

        all_match = True
        for filename in files_to_compare:
            if not self.compare_files(filename):
                all_match = False

        return all_match


def main():
    parser = argparse.ArgumentParser(description="Compare captured Mirror Node responses")
    parser.add_argument("dir1", help="First captured responses directory (MN1)")
    parser.add_argument("dir2", help="Second captured responses directory (MN2)")
    parser.add_argument("--output", "-o", help="Output report file (JSON)")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    dir1 = Path(args.dir1)
    dir2 = Path(args.dir2)

    if not dir1.exists():
        print(f"Error: Directory not found: {dir1}", file=sys.stderr)
        return 1
    if not dir2.exists():
        print(f"Error: Directory not found: {dir2}", file=sys.stderr)
        return 1

    print(f"Comparing captured Mirror Node responses")
    print(f"  Dir 1: {dir1}")
    print(f"  Dir 2: {dir2}")
    print()

    comparator = OfflineComparator(dir1, dir2, verbose=args.verbose)
    all_match = comparator.compare_all()

    print()
    print("=" * 60)
    if all_match:
        print("✅ PASS - Mirror Nodes produced IDENTICAL responses")
    else:
        print("❌ FAIL - Differences found")
        print(f"\nDifferences ({len(comparator.differences)}):")
        for diff in comparator.differences:
            if isinstance(diff, str):
                print(f"  - {diff}")
            else:
                print(f"  - {diff['file']}: {diff.get('details', 'unknown')}")

    # Save report
    if args.output:
        report = {
            "result": "pass" if all_match else "fail",
            "differences": comparator.differences,
            "dir1": str(dir1),
            "dir2": str(dir2)
        }
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\nReport saved to: {args.output}")

    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())
