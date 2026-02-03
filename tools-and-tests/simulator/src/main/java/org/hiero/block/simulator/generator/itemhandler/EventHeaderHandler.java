// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.hedera.hapi.block.stream.input.protoc.EventHeader;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.platform.event.legacy.EventCore;

/**
 * Handler for event headers in the block stream.
 * Creates and manages event header items containing metadata about events.
 */
public class EventHeaderHandler extends AbstractBlockItemHandler {
    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem =
                    BlockItem.newBuilder().setEventHeader(createEventHeader()).build();
        }
        return blockItem;
    }

    private EventHeader createEventHeader() {
        return EventHeader.newBuilder().setEventCore(createEventCore()).build();
    }

    private EventCore createEventCore() {
        return EventCore.newBuilder()
                .setCreatorNodeId(1) // Fixed node ID for deterministic consistency in tests
                .build();
    }
}
