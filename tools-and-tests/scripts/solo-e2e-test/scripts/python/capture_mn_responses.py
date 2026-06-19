#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Capture Mirror Node API responses to files for offline comparison.

Usage:
    python3 capture_mn_responses.py <mn_url> <output_dir> [--block-range START:END]

Example:
    python3 capture_mn_responses.py http://localhost:5551/api/v1 /tmp/mn1-responses --block-range 0:100
"""

import argparse
import json
import os
import sys
from pathlib import Path
import requests


def capture_endpoint(url: str, output_file: Path, params=None):
    """Capture API endpoint response to file."""
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"✓ Captured: {output_file.name}")
        return True
    except Exception as e:
        print(f"✗ Failed to capture {url}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Capture Mirror Node API responses")
    parser.add_argument("mn_url", help="Mirror Node API base URL")
    parser.add_argument("output_dir", help="Output directory for captured responses")
    parser.add_argument("--block-range", help="Block range (e.g., 0:100)", default=None)
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    mn_url = args.mn_url.rstrip('/')
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Capturing Mirror Node API responses")
    print(f"  From: {mn_url}")
    print(f"  To:   {output_dir}")
    print()

    success_count = 0
    total_count = 0

    # Parse block range
    start_block = 0
    end_block = 100
    if args.block_range:
        parts = args.block_range.split(':')
        start_block = int(parts[0])
        end_block = int(parts[1]) if len(parts) > 1 else 100

    # Capture network info
    total_count += 1
    if capture_endpoint(f"{mn_url}/network/nodes", output_dir / "network_nodes.json"):
        success_count += 1

    # Capture blocks
    total_count += 1
    if capture_endpoint(
        f"{mn_url}/blocks",
        output_dir / "blocks.json",
        params={"limit": end_block - start_block + 1, "order": "asc"}
    ):
        success_count += 1

    # Capture transactions
    total_count += 1
    if capture_endpoint(
        f"{mn_url}/transactions",
        output_dir / "transactions.json",
        params={"limit": 1000, "order": "asc"}
    ):
        success_count += 1

    # Capture balances
    total_count += 1
    if capture_endpoint(
        f"{mn_url}/balances",
        output_dir / "balances.json",
        params={"limit": 1000}
    ):
        success_count += 1

    # Save metadata
    metadata = {
        "mn_url": mn_url,
        "block_range": f"{start_block}:{end_block}",
        "captured_at": __import__('datetime').datetime.now().isoformat(),
    }
    with open(output_dir / "metadata.json", 'w') as f:
        json.dump(metadata, f, indent=2)

    print()
    print(f"Captured {success_count}/{total_count} endpoints")
    print(f"Output: {output_dir}")

    return 0 if success_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())
