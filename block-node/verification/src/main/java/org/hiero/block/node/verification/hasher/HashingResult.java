// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.hasher;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// A result returned after a successful hashing of a block.
/// @param blockNumber the number of the block, cannot be negative
/// @param blockSource the source of the block, cannot be null
/// @param block the whole block, cannot be null
/// @param rootHash the root hash of the block, cannot be null
/// @param blockHeader the parsed header of the block, cannot be null
/// @param blockFooter the parsed footer of the block, cannot be null
/// @param blockProofs all block proofs of the block, parsed, cannot be null
/// @param hapiProtoVersion the semantic HAPI version of the block, cannot be null
/// @param signedWRBPayload the raw record file item proto bytes if the block is WRB, null if not WRB
public record HashingResult(
        long blockNumber,
        BlockSource blockSource,
        BlockUnparsed block,
        Bytes rootHash,
        BlockHeader blockHeader,
        BlockFooter blockFooter,
        List<BlockProof> blockProofs,
        SemanticVersion hapiProtoVersion,
        byte[] signedWRBPayload) {
    public HashingResult {
        Objects.requireNonNull(blockSource, "Block source cannot be null");
        Objects.requireNonNull(block, "Block cannot be null");
        Objects.requireNonNull(rootHash, "Block root hash cannot be null");
        Objects.requireNonNull(blockHeader, "Block header cannot be null");
        Objects.requireNonNull(blockFooter, "Block footer cannot be null");
        Objects.requireNonNull(hapiProtoVersion, "HapiVersion cannot be null");
        if (blockNumber < 0) {
            throw new IllegalArgumentException("Block number cannot be negative");
        }
        if (blockProofs == null) {
            throw new IllegalArgumentException("Block proofs cannot be null or empty");
        }
    }
}
