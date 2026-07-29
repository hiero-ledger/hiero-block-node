#!/usr/bin/python3
# SPDX-License-Identifier: Apache-2.0
"""
Read a jumpstart binary file (as produced by `blocks wrap`) and pretty-print
its fields.

Layout (all big-endian):
    blockNumber                8 bytes  (long)
    blockHash                  48 bytes (SHA-384)
    consensusTimestampHash     48 bytes (SHA-384 leaf hash of the block's first consensus timestamp)
    outputItemsTreeRootHash    48 bytes (streaming merkle root of all output items)
    leafCount                  8 bytes  (long)
    hashCount                  4 bytes  (int)
    hashes                     48 * hashCount bytes (streaming hasher's open-root state)

Usage:
    extractJumpstartData.py /path/to/jumpstart.bin
"""

import argparse
import struct
import sys
from dataclasses import dataclass


@dataclass
class Jumpstart:
    block_number: int
    block_hash: str
    consensus_timestamp_hash: str
    output_items_tree_root_hash: str
    leaf_count: int
    hash_count: int
    hashes: list


def parse_args():
    parser = argparse.ArgumentParser(
        description='Read a jumpstart binary file and print its fields: '
                    'blockNumber, blockHash, consensusTimestampHash, '
                    'outputItemsTreeRootHash, leafCount, hashCount, and each hash.')
    parser.add_argument('filename', help='path to the jumpstart .bin file to read')
    return parser.parse_args()


def read_jumpstart(binary_file):
    block_number = struct.unpack('>q', binary_file.read(8))[0]
    block_hash = binary_file.read(48).hex()
    consensus_timestamp_hash = binary_file.read(48).hex()
    output_items_tree_root_hash = binary_file.read(48).hex()
    leaf_count = struct.unpack('>q', binary_file.read(8))[0]
    hash_count = struct.unpack('>i', binary_file.read(4))[0]
    hashes = [binary_file.read(48).hex() for _ in range(hash_count)]
    return Jumpstart(block_number, block_hash, consensus_timestamp_hash,
                     output_items_tree_root_hash, leaf_count, hash_count, hashes)


def print_jumpstart(jumpstart):
    print('blockNumber:', jumpstart.block_number)
    print('blockHash:', jumpstart.block_hash)
    print('consensusTimestampHash:', jumpstart.consensus_timestamp_hash)
    print('outputItemsTreeRootHash:', jumpstart.output_items_tree_root_hash)
    print('leafCount:', jumpstart.leaf_count)
    print('hashCount:', jumpstart.hash_count)
    for index, hash_hex in enumerate(jumpstart.hashes):
        print('  hash[' + str(index) + ']: ' + hash_hex)


def main():
    args = parse_args()
    try:
        binary_file = open(args.filename, 'rb')
    except OSError as err:
        print("error: could not open '{}': {}".format(args.filename, err.strerror),
              file=sys.stderr)
        return 1
    try:
        jumpstart = read_jumpstart(binary_file)
    finally:
        binary_file.close()
    print_jumpstart(jumpstart)
    return 0


if __name__ == '__main__':
    sys.exit(main())
