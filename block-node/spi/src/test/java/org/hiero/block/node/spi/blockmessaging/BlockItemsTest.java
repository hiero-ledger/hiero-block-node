// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.OneOf;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Test class for {@link BlockItems}.
 */
public class BlockItemsTest {

    private List<BlockItemUnparsed> blockItemList;

    @BeforeEach
    void setUp() {
        blockItemList = List.of(
                new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null)),
                new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null)));
    }

    @Test
    @DisplayName("Test BlockItems constructor with valid inputs")
    void testBlockItemsConstructorValid() {
        BlockItems blockItems = new BlockItems(blockItemList, 1);
        assertNotNull(blockItems);
        assertEquals(blockItemList, blockItems.blockItems());
        assertEquals(1, blockItems.blockNumber());
    }

    @Test
    @DisplayName("Test BlockItems constructor with empty block items list")
    void testBlockItemsConstructorEmptyList() {
        Executable executable = () -> new BlockItems(List.of(), 1);
        assertThrows(IllegalArgumentException.class, executable, "Block items cannot be empty");
    }

    @Test
    @DisplayName("Test BlockItems constructor with negative block number")
    void testBlockItemsConstructorNegativeBlockNumber() {
        Executable executable = () -> new BlockItems(blockItemList, -2);
        assertThrows(
                IllegalArgumentException.class,
                executable,
                "Block number cannot be negative unless it is UNKNOWN_BLOCK_NUMBER");
    }

    @Test
    @DisplayName("Test isStartOfNewBlock method")
    void testIsStartOfNewBlock() {
        BlockItems blockItems =
                new BlockItems(List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null))), 1);
        assertTrue(blockItems.isStartOfNewBlock());

        BlockItems blockItemsUnknown = new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_PROOF, null))), UNKNOWN_BLOCK_NUMBER);
        assertFalse(blockItemsUnknown.isStartOfNewBlock());
    }

    @Test
    @DisplayName("Test isEndOfBlock method")
    void testIsEndOfBlock() {
        BlockItemUnparsed blockItemWithProof = new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_PROOF, null));
        BlockItems blockItems = new BlockItems(List.of(blockItemWithProof), 1);
        assertTrue(blockItems.isEndOfBlock());
        BlockItems blockItemsWithoutProof = new BlockItems(blockItemList, 1);
        assertFalse(blockItemsWithoutProof.isEndOfBlock());
    }
}
