// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

    /**
     * Converts a BlockItem to a BlockItemUnparsed
     *
     * @param blockItem the BlockItem to convert
     * @return the BlockItemUnparsed representation of the BlockItem
     */
    public static BlockItemUnparsed toBlockItemUnparsed(BlockItem blockItem) {
        try {
            return BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(blockItem));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts BlockItems to a List of BlockItemUnparsed
     *
     * @param blockItem the BlockItems to convert
     * @return List of BlockItemUnparsed representation of the BlockItem
     */
    public static List<BlockItemUnparsed> toBlockItemsUnparsed(BlockItem... blockItem) {
        return Arrays.stream(blockItem).map(BlockItemUtils::toBlockItemUnparsed).toList();
    }

    /**
     * Converts a BlockItemUnparsed to a BlockItem
     *
     * @param blockItem the BlockItemUnparsed to convert
     * @return the BlockItem representation of the BlockItem
     */
    public static BlockItem toBlockItem(BlockItemUnparsed blockItem) {
        try {
            return BlockItem.PROTOBUF.parse(BlockItemUnparsed.PROTOBUF.toBytes(blockItem));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts BlockItemUnparsed to a List of BlockItem
     *
     * @param blockItemsUnparsed the BlockItemUnparsed to convert
     * @return List of BlockItem representation of the BlockItem
     */
    public static List<BlockItem> toBlockItems(BlockItemUnparsed... blockItemsUnparsed) {
        return Arrays.stream(blockItemsUnparsed)
                .map(BlockItemUtils::toBlockItem)
                .toList();
    }

    /**
     * Converts BlockItemUnparsed to a List of BlockItem
     *
     * @param blockItemsUnparsed the BlockItemUnparsed to convert
     * @return List of BlockItem representation of the BlockItem
     */
    public static List<BlockItem> toBlockItems(Collection<BlockItemUnparsed> blockItemsUnparsed) {
        return blockItemsUnparsed.stream().map(BlockItemUtils::toBlockItem).toList();
    }
}
