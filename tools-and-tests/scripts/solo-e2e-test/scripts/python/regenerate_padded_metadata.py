#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Regenerate block_times.bin and day_blocks.json with padding for wrap retry."""

import sys
import gzip
from pathlib import Path
from collections import defaultdict
import struct
import json
from datetime import datetime, timezone

def main():
    if len(sys.argv) != 6:
        print("Usage: regenerate_padded_metadata.py <records_dir> <block_times_file> <day_blocks_file> <genesis_epoch_nanos> <starting_block_num>", file=sys.stderr)
        sys.exit(1)

    records_dir = sys.argv[1]
    block_times_file = sys.argv[2]
    day_blocks_file = sys.argv[3]
    genesis_epoch_nanos = int(sys.argv[4])
    starting_block_num = int(sys.argv[5])

    # Find all record files (search recursively)
    record_files = sorted(list(Path(records_dir).glob('**/*.rcd.gz')) + list(Path(records_dir).glob('**/*.rcd')))

    if not record_files:
        print("No record files found", file=sys.stderr)
        sys.exit(1)

    print(f"Regenerating metadata for {len(record_files)} files starting from block {starting_block_num}...", file=sys.stderr)

    blocks_data = []
    for idx, rcd_file in enumerate(record_files):
        filename = rcd_file.name
        timestamp = filename.split('.rcd')[0]
        block_num = starting_block_num + idx

        # Convert timestamp to epoch nanos
        iso_timestamp = timestamp.replace('_', ':')
        datetime_part = iso_timestamp.split('.')[0]
        nanos_part = iso_timestamp.split('.')[1].rstrip('Z')

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

    # Write block_times.bin
    # Pad from block 0 to starting_block_num-1 with extrapolated timestamps
    with open(block_times_file, 'wb') as f:
        if starting_block_num > 0 and len(blocks_data) > 0:
            # Calculate average block time for extrapolation
            if len(blocks_data) >= 2:
                avg_block_time = (blocks_data[1]['relative_nanos'] - blocks_data[0]['relative_nanos'])
            else:
                avg_block_time = 1_000_000_000  # 1 second default

            # Pad blocks 0 to starting_block_num-1 with extrapolated timestamps
            first_block_time = blocks_data[0]['relative_nanos']
            for i in range(starting_block_num):
                padded_time = max(0, first_block_time - (starting_block_num - i) * avg_block_time)
                f.seek(i * 8)
                f.write(struct.pack('>Q', padded_time))

        # Write actual block data
        for block in blocks_data:
            f.seek(block['block_num'] * 8)
            f.write(struct.pack('>Q', block['relative_nanos']))

    # Generate day_blocks.json
    # Use block 0 as first since we padded from 0
    days = defaultdict(lambda: {'first': None, 'last': None})
    for block in blocks_data:
        day = block['day']
        if days[day]['first'] is None:
            # Set first block to 0 if we padded, otherwise use actual block number
            days[day]['first'] = 0 if starting_block_num > 0 else block['block_num']
        days[day]['last'] = block['block_num']

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

    print(f"Regenerated metadata: blocks 0 to {starting_block_num + len(record_files) - 1} (padded 0-{starting_block_num-1}, actual {starting_block_num}-{starting_block_num + len(record_files) - 1})", file=sys.stderr)

if __name__ == '__main__':
    main()
