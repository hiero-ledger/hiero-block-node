// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collections;
import org.hiero.block.node.spi.blockmessaging.BlockNotification.Type;
import org.hiero.hapi.block.node.BlockUnparsed;
import org.junit.jupiter.api.Test;

/**
 * AI Generated unit tests for the BlockNotification class to keep coverage happy.
 */
public class BlockNotificationTest {

    @Test
    void testBlockNotificationCreationValidation() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockUnparsed block = new BlockUnparsed(Collections.emptyList());
        assertThrows(
                IllegalArgumentException.class, () -> new BlockNotification(1L, Type.BLOCK_VERIFIED, blockHash, null));
        assertThrows(IllegalArgumentException.class, () -> new BlockNotification(1L, Type.BLOCK_VERIFIED, null, block));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockNotification(1L, Type.BLOCK_FAILED_VERIFICATION, blockHash, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BlockNotification(1L, Type.BLOCK_FAILED_VERIFICATION, blockHash, block));
    }

    @Test
    void testBlockNotificationCreation() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockUnparsed block = new BlockUnparsed(Collections.emptyList());
        BlockNotification notification =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash, block);
        assertEquals(1L, notification.blockNumber());
        assertEquals(BlockNotification.Type.BLOCK_VERIFIED, notification.type());
        assertEquals(blockHash, notification.blockHash());
        assertEquals(block, notification.block());
    }

    @Test
    void testBlockNotificationEquality() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockUnparsed block = new BlockUnparsed(Collections.emptyList());
        BlockNotification notification1 =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash, block);
        BlockNotification notification2 =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash, block);
        assertEquals(notification1, notification2);
    }

    @Test
    void testBlockNotificationInequality() {
        Bytes blockHash1 = Bytes.wrap(new byte[] {1, 2, 3});
        BlockUnparsed block = new BlockUnparsed(Collections.emptyList());
        BlockNotification notification1 =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash1, block);
        BlockNotification notification2 =
                new BlockNotification(2L, BlockNotification.Type.BLOCK_FAILED_VERIFICATION, null, null);
        assertNotEquals(notification1, notification2);
    }

    @Test
    void testBlockNotificationHashCode() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockUnparsed block = new BlockUnparsed(Collections.emptyList());
        BlockNotification notification1 =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash, block);
        BlockNotification notification2 =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash, block);
        assertEquals(notification1.hashCode(), notification2.hashCode());
    }

    @Test
    void testBlockNotificationToString() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockUnparsed block = new BlockUnparsed(Collections.emptyList());
        BlockNotification notification =
                new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash, block);
        String expected = "BlockNotification[blockNumber=1, type=BLOCK_VERIFIED, blockHash=" + blockHash
                + ", block=BlockUnparsed[blockItems=[]]]";
        assertEquals(expected, notification.toString());
    }
}
