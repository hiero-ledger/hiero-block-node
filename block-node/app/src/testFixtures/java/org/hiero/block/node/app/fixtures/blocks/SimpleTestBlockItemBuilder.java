// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collections;

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 */
public class SimpleTestBlockItemBuilder {

    public static BlockItem sampleBlockHeader(long blockNumber) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_HEADER,
                new BlockHeader(
                        new SemanticVersion(1, 2, 3, "a", "b"),
                        new SemanticVersion(4, 5, 6, "c", "d"),
                        blockNumber,
                        Bytes.wrap("hash".getBytes()),
                        new Timestamp(123L, 456),
                        BlockHashAlgorithm.SHA2_384)));
    }

    public static BlockItem sampleRoundHeader(long roundNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, new RoundHeader(roundNumber)));
    }

    public static BlockItem sampleBlockProof(long blockNumber) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_PROOF,
                new BlockProof(
                        blockNumber,
                        Bytes.wrap("previousBlockRootHash".getBytes()),
                        Bytes.wrap("startOfBlockStateRootHash".getBytes()),
                        Bytes.wrap("signature".getBytes()),
                        Collections.emptyList())));
    }
}
