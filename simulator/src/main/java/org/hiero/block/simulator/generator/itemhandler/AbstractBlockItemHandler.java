// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hederahashgraph.api.proto.java.SemanticVersion;
import com.hederahashgraph.api.proto.java.Timestamp;
import java.util.Random;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/**
 * Abstract base class for block item handlers providing common functionality.
 * Implements core methods and utilities used by specific item handlers.
 */
abstract class AbstractBlockItemHandler implements ItemHandler {
    protected BlockItem blockItem;

    @Override
    public BlockItem getItem() {
        return blockItem;
    }

    @Override
    public BlockItemUnparsed unparseBlockItem() throws BlockSimulatorParsingException {
        try {
            return BlockItemUnparsed.PROTOBUF.parse(Bytes.wrap(getItem().toByteArray()));
        } catch (ParseException e) {
            throw new BlockSimulatorParsingException(e);
        }
    }

    /**
     * Creates a timestamp representing the current time.
     *
     * @return A Timestamp protobuf object
     */
    protected Timestamp getTimestamp() {
        return Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .build();
    }

    /**
     * Creates a semantic version object with default values.
     *
     * @return A SemanticVersion protobuf object
     */
    protected SemanticVersion getSemanticVersion() {
        return SemanticVersion.newBuilder().setMajor(0).setMinor(1).setPatch(0).build();
    }

    /**
     * Generates a random value within the specified range.
     *
     * @param min The minimum value (inclusive)
     * @param max The maximum value (exclusive)
     * @return A random long value between min and max
     * @throws IllegalArgumentException if min or max is negative
     */
    protected long generateRandomValue(long min, long max) {
        Preconditions.requirePositive(min);
        Preconditions.requirePositive(max);

        return new Random().nextLong(min, max);
    }
}
