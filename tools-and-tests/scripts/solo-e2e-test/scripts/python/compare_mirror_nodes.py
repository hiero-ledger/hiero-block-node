#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Mirror Node API Comparison Tool

Compares two Mirror Node REST APIs to validate that they produce identical results
when ingesting live blocks vs WRB CLI-wrapped blocks.

Usage:
    python3 compare_mirror_nodes.py <mn1_url> <mn2_url> [--block-range START:END] [--output report.json]

Example:
    python3 compare_mirror_nodes.py \
        http://localhost:5551/api/v1 \
        http://localhost:5552/api/v1 \
        --block-range 0:100 \
        --output comparison-report.json
"""

import argparse
import json
import sys
import time
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin
import requests


class MirrorNodeComparator:
    """Compares two Mirror Node APIs for identical responses."""

    def __init__(self, mn1_url: str, mn2_url: str, verbose: bool = False):
        """
        Initialize comparator.

        Args:
            mn1_url: Mirror Node 1 API base URL (e.g., http://localhost:5551/api/v1)
            mn2_url: Mirror Node 2 API base URL (e.g., http://localhost:5552/api/v1)
            verbose: Enable verbose logging
        """
        self.mn1_url = mn1_url.rstrip('/')
        self.mn2_url = mn2_url.rstrip('/')
        self.verbose = verbose
        self.differences = []
        self.warnings = []

    def log(self, message: str):
        """Log message if verbose mode is enabled."""
        if self.verbose:
            print(f"[compare] {message}", file=sys.stderr)

    def warn(self, message: str):
        """Log warning message."""
        self.warnings.append(message)
        print(f"[WARNING] {message}", file=sys.stderr)

    def error(self, message: str):
        """Log error message."""
        print(f"[ERROR] {message}", file=sys.stderr)

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """
        Make GET request to Mirror Node API.

        Args:
            url: Full URL to query
            params: Query parameters
            timeout: Request timeout in seconds

        Returns:
            JSON response as dict, or None on error
        """
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.error(f"Request failed: {url} - {e}")
            return None

    def compare_responses(self, endpoint: str, mn1_data: Any, mn2_data: Any, context: str = "") -> bool:
        """
        Compare two API responses for equality.

        Args:
            endpoint: API endpoint being compared
            mn1_data: Response from MN1
            mn2_data: Response from MN2
            context: Additional context for error reporting

        Returns:
            True if responses match, False otherwise
        """
        if mn1_data == mn2_data:
            return True

        # Record difference
        difference = {
            "endpoint": endpoint,
            "context": context,
            "mn1_data": mn1_data,
            "mn2_data": mn2_data,
            "type": self._classify_difference(mn1_data, mn2_data)
        }
        self.differences.append(difference)
        self.log(f"Difference found in {endpoint} {context}: {difference['type']}")
        return False

    def _classify_difference(self, data1: Any, data2: Any) -> str:
        """Classify the type of difference between two data structures."""
        if type(data1) != type(data2):
            return f"type_mismatch ({type(data1).__name__} vs {type(data2).__name__})"

        if isinstance(data1, dict):
            keys1 = set(data1.keys())
            keys2 = set(data2.keys())
            if keys1 != keys2:
                missing_in_mn2 = keys1 - keys2
                missing_in_mn1 = keys2 - keys1
                return f"key_mismatch (MN1 extra: {missing_in_mn2}, MN2 extra: {missing_in_mn1})"
            return "value_mismatch"

        if isinstance(data1, list):
            if len(data1) != len(data2):
                return f"length_mismatch ({len(data1)} vs {len(data2)})"
            return "element_mismatch"

        return f"value_mismatch ({data1} vs {data2})"

    def compare_network_info(self) -> bool:
        """Compare /network/nodes endpoint."""
        self.log("Comparing /network/nodes...")

        mn1_nodes = self.get(f"{self.mn1_url}/network/nodes")
        mn2_nodes = self.get(f"{self.mn2_url}/network/nodes")

        if mn1_nodes is None or mn2_nodes is None:
            self.error("Failed to fetch network nodes")
            return False

        return self.compare_responses("/network/nodes", mn1_nodes, mn2_nodes)

    def compare_blocks(self, start_block: int = 0, end_block: Optional[int] = None, limit: int = 100) -> bool:
        """
        Compare /blocks endpoint for a block range.

        Args:
            start_block: First block number to compare
            end_block: Last block number to compare (None = latest)
            limit: Number of blocks to fetch per page

        Returns:
            True if all blocks match, False otherwise
        """
        self.log(f"Comparing /blocks from {start_block} to {end_block or 'latest'}...")

        all_match = True
        next_link = None

        # Build initial query
        params = {"limit": limit, "order": "asc"}
        if start_block > 0:
            params["block.number"] = f"gte:{start_block}"

        while True:
            # Fetch from MN1
            if next_link:
                mn1_blocks = self.get(next_link)
            else:
                mn1_blocks = self.get(f"{self.mn1_url}/blocks", params=params)

            # Fetch from MN2 (same params)
            if next_link:
                # Replace MN1 URL with MN2 URL in next link
                mn2_next = next_link.replace(self.mn1_url, self.mn2_url)
                mn2_blocks = self.get(mn2_next)
            else:
                mn2_blocks = self.get(f"{self.mn2_url}/blocks", params=params)

            if mn1_blocks is None or mn2_blocks is None:
                self.error("Failed to fetch blocks")
                return False

            # Compare blocks
            mn1_items = mn1_blocks.get("blocks", [])
            mn2_items = mn2_blocks.get("blocks", [])

            if len(mn1_items) != len(mn2_items):
                all_match = False
                self.warn(f"Block count mismatch: MN1 has {len(mn1_items)}, MN2 has {len(mn2_items)}")

            # Compare each block
            for i, (b1, b2) in enumerate(zip(mn1_items, mn2_items)):
                block_num = b1.get("number", "unknown")
                if not self.compare_responses("/blocks", b1, b2, f"block {block_num}"):
                    all_match = False

                # Stop at end_block if specified
                if end_block is not None and block_num >= end_block:
                    self.log(f"Reached end block {end_block}")
                    return all_match

            # Check for next page
            next_link = mn1_blocks.get("links", {}).get("next")
            if not next_link:
                break

            self.log(f"Fetching next page: {len(mn1_items)} blocks processed...")

        return all_match

    def compare_transactions(self, start_timestamp: Optional[str] = None,
                           end_timestamp: Optional[str] = None,
                           limit: int = 100) -> bool:
        """
        Compare /transactions endpoint.

        Args:
            start_timestamp: Start timestamp (e.g., "1234567890.000000000")
            end_timestamp: End timestamp
            limit: Number of transactions to fetch per page

        Returns:
            True if all transactions match, False otherwise
        """
        self.log("Comparing /transactions...")

        all_match = True
        next_link = None

        # Build initial query
        params = {"limit": limit, "order": "asc"}
        if start_timestamp:
            params["timestamp"] = f"gte:{start_timestamp}"
        if end_timestamp:
            if "timestamp" in params:
                params["timestamp"] += f"&timestamp=lte:{end_timestamp}"
            else:
                params["timestamp"] = f"lte:{end_timestamp}"

        page_count = 0
        tx_count = 0

        while True:
            # Fetch from both MNs
            if next_link:
                mn1_txs = self.get(next_link)
                mn2_next = next_link.replace(self.mn1_url, self.mn2_url)
                mn2_txs = self.get(mn2_next)
            else:
                mn1_txs = self.get(f"{self.mn1_url}/transactions", params=params)
                mn2_txs = self.get(f"{self.mn2_url}/transactions", params=params)

            if mn1_txs is None or mn2_txs is None:
                self.error("Failed to fetch transactions")
                return False

            # Compare transactions
            mn1_items = mn1_txs.get("transactions", [])
            mn2_items = mn2_txs.get("transactions", [])

            if len(mn1_items) != len(mn2_items):
                all_match = False
                self.warn(f"Transaction count mismatch on page {page_count}: MN1 has {len(mn1_items)}, MN2 has {len(mn2_items)}")

            # Compare each transaction
            for i, (tx1, tx2) in enumerate(zip(mn1_items, mn2_items)):
                tx_id = tx1.get("transaction_id", "unknown")
                if not self.compare_responses("/transactions", tx1, tx2, f"tx {tx_id}"):
                    all_match = False

            tx_count += len(mn1_items)
            page_count += 1

            # Check for next page
            next_link = mn1_txs.get("links", {}).get("next")
            if not next_link:
                break

            self.log(f"Processed {tx_count} transactions ({page_count} pages)...")

        self.log(f"Compared {tx_count} transactions total")
        return all_match

    def compare_balances(self, timestamp: Optional[str] = None, limit: int = 100) -> bool:
        """
        Compare /balances endpoint.

        Args:
            timestamp: Balance timestamp (default: latest)
            limit: Number of accounts to fetch per page

        Returns:
            True if all balances match, False otherwise
        """
        self.log("Comparing /balances...")

        all_match = True
        next_link = None

        params = {"limit": limit, "order": "asc"}
        if timestamp:
            params["timestamp"] = timestamp

        account_count = 0

        while True:
            # Fetch from both MNs
            if next_link:
                mn1_balances = self.get(next_link)
                mn2_next = next_link.replace(self.mn1_url, self.mn2_url)
                mn2_balances = self.get(mn2_next)
            else:
                mn1_balances = self.get(f"{self.mn1_url}/balances", params=params)
                mn2_balances = self.get(f"{self.mn2_url}/balances", params=params)

            if mn1_balances is None or mn2_balances is None:
                self.error("Failed to fetch balances")
                return False

            # Compare balances
            mn1_items = mn1_balances.get("balances", [])
            mn2_items = mn2_balances.get("balances", [])

            if len(mn1_items) != len(mn2_items):
                all_match = False
                self.warn(f"Balance count mismatch: MN1 has {len(mn1_items)}, MN2 has {len(mn2_items)}")

            # Compare each account balance
            for i, (bal1, bal2) in enumerate(zip(mn1_items, mn2_items)):
                account_id = bal1.get("account", "unknown")
                if not self.compare_responses("/balances", bal1, bal2, f"account {account_id}"):
                    all_match = False

            account_count += len(mn1_items)

            # Check for next page
            next_link = mn1_balances.get("links", {}).get("next")
            if not next_link:
                break

            self.log(f"Processed {account_count} account balances...")

        self.log(f"Compared {account_count} account balances total")
        return all_match

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate comparison report.

        Returns:
            Report dictionary with results and statistics
        """
        report = {
            "summary": {
                "mn1_url": self.mn1_url,
                "mn2_url": self.mn2_url,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                "match": len(self.differences) == 0,
                "difference_count": len(self.differences),
                "warning_count": len(self.warnings)
            },
            "differences": self.differences,
            "warnings": self.warnings
        }

        return report


def main():
    parser = argparse.ArgumentParser(
        description="Compare two Mirror Node REST APIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument("mn1_url", help="Mirror Node 1 API base URL (e.g., http://localhost:5551/api/v1)")
    parser.add_argument("mn2_url", help="Mirror Node 2 API base URL (e.g., http://localhost:5552/api/v1)")
    parser.add_argument("--block-range", help="Block range to compare (e.g., 0:100)", default=None)
    parser.add_argument("--output", "-o", help="Output report file (JSON)", default=None)
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--skip-network", action="store_true", help="Skip network info comparison")
    parser.add_argument("--skip-blocks", action="store_true", help="Skip blocks comparison")
    parser.add_argument("--skip-transactions", action="store_true", help="Skip transactions comparison")
    parser.add_argument("--skip-balances", action="store_true", help="Skip balances comparison")

    args = parser.parse_args()

    # Create comparator
    comparator = MirrorNodeComparator(args.mn1_url, args.mn2_url, verbose=args.verbose)

    print(f"Comparing Mirror Node APIs:")
    print(f"  MN1: {args.mn1_url}")
    print(f"  MN2: {args.mn2_url}")
    print()

    all_match = True

    # Parse block range if specified
    start_block = 0
    end_block = None
    if args.block_range:
        try:
            parts = args.block_range.split(':')
            start_block = int(parts[0])
            end_block = int(parts[1]) if len(parts) > 1 and parts[1] else None
            print(f"Block range: {start_block} to {end_block or 'latest'}")
        except ValueError:
            print(f"ERROR: Invalid block range format: {args.block_range}", file=sys.stderr)
            return 1

    # Run comparisons
    if not args.skip_network:
        print("Comparing network info...")
        if not comparator.compare_network_info():
            all_match = False
            print("  ❌ Network info mismatch")
        else:
            print("  ✅ Network info matches")

    if not args.skip_blocks:
        print("Comparing blocks...")
        if not comparator.compare_blocks(start_block=start_block, end_block=end_block):
            all_match = False
            print("  ❌ Blocks mismatch")
        else:
            print("  ✅ Blocks match")

    if not args.skip_transactions:
        print("Comparing transactions...")
        if not comparator.compare_transactions():
            all_match = False
            print("  ❌ Transactions mismatch")
        else:
            print("  ✅ Transactions match")

    if not args.skip_balances:
        print("Comparing balances...")
        if not comparator.compare_balances():
            all_match = False
            print("  ❌ Balances mismatch")
        else:
            print("  ✅ Balances match")

    # Generate report
    report = comparator.generate_report()

    # Print summary
    print()
    print("=" * 60)
    print("COMPARISON SUMMARY")
    print("=" * 60)
    print(f"Result: {'✅ PASS - Mirror Nodes are IDENTICAL' if all_match else '❌ FAIL - Differences found'}")
    print(f"Differences: {len(comparator.differences)}")
    print(f"Warnings: {len(comparator.warnings)}")

    # Print first few differences
    if comparator.differences:
        print()
        print("First 5 differences:")
        for diff in comparator.differences[:5]:
            print(f"  - {diff['endpoint']} {diff['context']}: {diff['type']}")

    # Save report if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\nDetailed report saved to: {args.output}")

    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())