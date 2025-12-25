// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hederahashgraph.api.proto.java.SemanticVersion;
import com.hederahashgraph.api.proto.java.Timestamp;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/**
 * Abstract base class for block item handlers providing common functionality.
 * Implements core methods and utilities used by specific item handlers.
 */
abstract class AbstractBlockItemHandler implements ItemHandler {

    static final Timestamp FIXED_TIMESTAMP = Timestamp.newBuilder()
            .setSeconds(1734953412) // Fixed timestamp for deterministic consistency in tests
            .build();

    protected BlockItem blockItem;

    /** Current HAPI version to craft blocks from **/
    final SemanticVersion HAPI_VERSION =
            SemanticVersion.newBuilder().setMajor(0).setMinor(69).setPatch(0).build();

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
        return FIXED_TIMESTAMP;
    }

    /**
     * Creates a semantic version object with default values.
     *
     * @return A SemanticVersion protobuf object
     */
    protected SemanticVersion getSemanticVersion() {
        return HAPI_VERSION;
    }
}
