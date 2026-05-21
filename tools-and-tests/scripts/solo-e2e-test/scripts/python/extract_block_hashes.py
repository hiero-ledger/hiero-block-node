#!/usr/bin/env python3
"""Extract block hashes from record files."""

import sys
import os
import gzip
import struct
from pathlib import Path

def parse_record_file(file_path):
    """Parse a record file to extract the block hash (last item's running hash)."""
    try:
        # Open file (handle .gz compression)
        if file_path.endswith('.gz'):
            f = gzip.open(file_path, 'rb')
        else:
            f = open(file_path, 'rb')

        data = f.read()
        f.close()

        # Record file format: [version(4)] [HAPI version(4)] [items...]
        # Each item: [length(4)] [type hash(48)] [protobuf data]
        # We want the last item's type hash (first 48 bytes after length)

        offset = 8  # Skip version headers
        last_hash = None

        while offset < len(data) - 52:  # Need at least 4 bytes length + 48 bytes hash
            # Read item length (big-endian int32)
            if offset + 4 > len(data):
                break
            item_length = struct.unpack('>I', data[offset:offset+4])[0]
            offset += 4

            # Read type hash (48 bytes)
            if offset + 48 > len(data):
                break
            type_hash = data[offset:offset+48]
            last_hash = type_hash.hex()

            # Skip the rest of the item
            offset += item_length

        return last_hash
    except Exception as e:
        import traceback
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def main():
    if len(sys.argv) != 3:
        print("Usage: extract_block_hashes.py <records_dir> <output_file>", file=sys.stderr)
        sys.exit(1)

    records_dir = sys.argv[1]
    output_file = sys.argv[2]

    # Find all record files and extract hashes
    record_files = sorted([
        f for f in Path(records_dir).glob('*.rcd.gz')
    ] + [
        f for f in Path(records_dir).glob('*.rcd')
    ])

    if not record_files:
        print("No record files found", file=sys.stderr)
        sys.exit(1)

    # Group by day and extract first/last hashes
    from collections import defaultdict
    day_data = defaultdict(list)

    for rcd_file in record_files:
        filename = rcd_file.name
        # Extract date: 2026-05-11T19_07_57.673598574Z.rcd.gz
        day = filename.split('T')[0]

        block_hash = parse_record_file(str(rcd_file))
        if block_hash:
            day_data[day].append((filename, block_hash))

    # Write hash data to output file
    with open(output_file, 'w') as f:
        for day in sorted(day_data.keys()):
            hashes = day_data[day]
            first_hash = hashes[0][1]
            last_hash = hashes[-1][1]
            f.write(f"{day}|{first_hash}|{last_hash}\n")

    print(f"Extracted hashes for {len(day_data)} days", file=sys.stderr)

if __name__ == '__main__':
    main()
