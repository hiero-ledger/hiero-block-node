// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.verifier;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// A result record for successful verification of a block's proofs.
/// @param blockNumber the number of the block, must be positive
/// @param rootHash the root hash of the block, cannot be null
/// @param block the block itself, cannot be null
/// @param source the source of the block, cannot be null
public record BlockVerificationResult(long blockNumber, Bytes rootHash, BlockUnparsed block, BlockSource source) {
    public BlockVerificationResult {
        if (blockNumber < 0) {
            throw new IllegalArgumentException("blockNumber cannot be negative for a block verification result");
        }
        Objects.requireNonNull(rootHash, "rootHash cannot be null for a block verification result");
        Objects.requireNonNull(block, "block cannot be null for a block verification result");
        Objects.requireNonNull(source, "blockSource cannot be null for a block verification result");
    }
}
