#!/usr/bin/env python3
"""Generate block_times.bin and day_blocks.json metadata from record files."""

import sys
import os
import gzip
import struct
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone

def extract_block_number_from_protobuf(data):
    """Extract block number from protobuf BlockItem data.

    The record file contains BlockItems. We need to find a BlockProof item
    which contains the block number.
    """
    offset = 0
    while offset < len(data) - 10:
        # Try to find block number in protobuf data
        # Block number is typically encoded as a varint early in the protobuf
        # For now, we'll use a simple heuristic: look for small varints near the start

        # Check if this looks like a varint (field number + value)
        if offset + 2 < len(data):
            # Try to decode as varint
            byte = data[offset]
            if byte & 0x80 == 0:  # Single byte varint
                # Check if next bytes form a reasonable block number
                if offset + 10 < len(data):
                    # Try to read next varint as block number
                    block_num_offset = offset + 1
                    block_num = 0
                    shift = 0
                    for i in range(10):  # Max 10 bytes for varint
                        if block_num_offset + i >= len(data):
                            break
                        b = data[block_num_offset + i]
                        block_num |= (b & 0x7F) << shift
                        if b & 0x80 == 0:
                            # Found complete varint
                            if 0 <= block_num < 1000000:  # Reasonable block number range
                                return block_num
                            break
                        shift += 7
        offset += 1

    return None

def parse_record_file_metadata(file_path):
    """Parse record file to extract block number and timestamp."""
    try:
        # Open file (handle .gz compression)
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rb') as f:
                data = f.read()
        else:
            with open(file_path, 'rb') as f:
                data = f.read()

        # Record file format: [version(4)] [HAPI version(4)] [items...]
        # Each item: [length(4)] [type hash(48)] [protobuf data]

        offset = 8  # Skip version headers
        block_num = None

        # Parse items to find block number
        while offset < len(data) - 52 and block_num is None:
            if offset + 4 > len(data):
                break
            item_length = struct.unpack('>I', data[offset:offset+4])[0]
            offset += 4

            if offset + 48 > len(data):
                break
            # Skip type hash
            offset += 48

            # Try to extract block number from protobuf data
            if offset + item_length <= len(data):
                item_data = data[offset:offset+item_length]
                extracted_num = extract_block_number_from_protobuf(item_data)
                if extracted_num is not None:
                    block_num = extracted_num
                    break

            offset += item_length

        return block_num

    except Exception as e:
        import traceback
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def main():
    if len(sys.argv) != 5:
        print("Usage: generate_metadata.py <records_dir> <block_times_file> <day_blocks_file> <genesis_epoch_nanos>", file=sys.stderr)
        sys.exit(1)

    records_dir = sys.argv[1]
    block_times_file = sys.argv[2]
    day_blocks_file = sys.argv[3]
    genesis_epoch_nanos = int(sys.argv[4])

    # Always use 0-based indexing for wrap command compatibility
    starting_block_num = 0

    # Find all record files (search recursively for files in subdirectories)
    record_files = sorted(list(Path(records_dir).glob('**/*.rcd.gz')) + list(Path(records_dir).glob('**/*.rcd')))

    if not record_files:
        print("No record files found", file=sys.stderr)
        sys.exit(1)

    print(f"Processing {len(record_files)} record files using sequential numbering from block {starting_block_num}...", file=sys.stderr)

    # Use sequential block numbering starting from starting_block_num
    blocks_data = []
    for idx, rcd_file in enumerate(record_files):
        filename = rcd_file.name
        # Extract timestamp from filename
        timestamp = filename.split('.rcd')[0]

        # Use sequential block numbering
        block_num = starting_block_num + idx

        # Convert timestamp to epoch nanos
        iso_timestamp = timestamp.replace('_', ':')
        datetime_part = iso_timestamp.split('.')[0]
        nanos_part = iso_timestamp.split('.')[1].rstrip('Z')

        # Use date parsing (simplified - assumes format is correct)
        # IMPORTANT: Timestamps are in UTC (indicated by 'Z' suffix), so use timezone.utc
        dt = datetime.strptime(datetime_part, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
        epoch_seconds = int(dt.timestamp())
        epoch_nanos = epoch_seconds * 1000000000 + int(nanos_part)
        relative_nanos = epoch_nanos - genesis_epoch_nanos

        blocks_data.append({
            'block_num': block_num,
            'timestamp': timestamp,
            'relative_nanos': relative_nanos,
            'day': timestamp.split('T')[0]
        })

    # Sort by block number to ensure correct ordering
    blocks_data.sort(key=lambda x: x['block_num'])

    print(f"Extracted block numbers: {blocks_data[0]['block_num']} to {blocks_data[-1]['block_num']}", file=sys.stderr)

    # Write block_times.bin
    with open(block_times_file, 'wb') as f:
        for block in blocks_data:
            # Write at position block_num * 8
            f.seek(block['block_num'] * 8)
            f.write(struct.pack('>Q', block['relative_nanos']))

    # Generate day_blocks.json
    days = defaultdict(lambda: {'first': None, 'last': None, 'day_str': ''})
    for block in blocks_data:
        day = block['day']
        if days[day]['first'] is None:
            days[day]['first'] = block['block_num']
            days[day]['day_str'] = day
        days[day]['last'] = block['block_num']

    # Format as JSON array
    day_entries = []
    for day in sorted(days.keys()):
        year, month, day_num = day.split('-')
        day_entries.append({
            'year': int(year),
            'month': int(month),
            'day': int(day_num),
            'firstBlockNumber': days[day]['first'],
            'lastBlockNumber': days[day]['last']
        })

    with open(day_blocks_file, 'w') as f:
        json.dump(day_entries, f, indent=2)

    print(f"Generated metadata: block_times.bin and day_blocks.json", file=sys.stderr)

if __name__ == '__main__':
    main()
