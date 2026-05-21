#!/usr/bin/env python3
"""Update day_blocks.json with extracted block hashes."""

import sys
import json

def main():
    if len(sys.argv) != 3:
        print("Usage: update_day_blocks_hashes.py <day_blocks_file> <hash_file>", file=sys.stderr)
        sys.exit(1)

    day_blocks_file = sys.argv[1]
    hash_file = sys.argv[2]

    # Read existing day_blocks.json
    with open(day_blocks_file, 'r') as f:
        day_blocks = json.load(f)

    # Read hash data
    hash_data = {}
    with open(hash_file, 'r') as f:
        for line in f:
            day, first_hash, last_hash = line.strip().split('|')
            hash_data[day] = {
                'firstBlockHash': first_hash,
                'lastBlockHash': last_hash
            }

    # Update day_blocks with hashes
    for entry in day_blocks:
        day_key = f"{entry['year']:04d}-{entry['month']:02d}-{entry['day']:02d}"
        if day_key in hash_data:
            entry['firstBlockHash'] = hash_data[day_key]['firstBlockHash']
            entry['lastBlockHash'] = hash_data[day_key]['lastBlockHash']
        else:
            print(f"Warning: No hash data for {day_key}", file=sys.stderr)

    # Write updated day_blocks.json
    with open(day_blocks_file, 'w') as f:
        json.dump(day_blocks, f, indent=2)

    print(f"Updated {len(day_blocks)} day entries with hashes", file=sys.stderr)

if __name__ == '__main__':
    main()
