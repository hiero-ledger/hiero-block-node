// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * Utility class for testing block items.
 */
public final class BlockItemUtils {
    /**
     * Converts a BlockItem to a JSON string
     *
     * @param blockItem the BlockItem to convert
     * @return the JSON string representation of the BlockItem
     */
    public static String toBlockItemJson(BlockItem blockItem) {
        return BlockItem.JSON.toJSON(blockItem);
    }

    /**
     * Converts a BlockItemUnparsed to BlockItem then a JSON string
     *
     * @param blockItemUnparsed the BlockItemUnparsed to convert
     * @return the JSON string representation of the BlockItemUnparsed as a BlockItem
     */
    public static String toBlockItemJson(BlockItemUnparsed blockItemUnparsed) {
        try {
            return BlockItem.JSON.toJSON(
                    BlockItem.PROTOBUF.parse(BlockItemUnparsed.PROTOBUF.toBytes(blockItemUnparsed)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
