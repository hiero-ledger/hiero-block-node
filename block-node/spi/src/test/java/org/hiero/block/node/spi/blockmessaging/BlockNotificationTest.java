// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.junit.jupiter.api.Test;

/**
 * AI Generated unit tests for the BlockNotification class to keep coverage happy.
 */
public class BlockNotificationTest {
    @Test
    void testBlockNotificationCreation() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockNotification notification = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash);
        assertEquals(1L, notification.blockNumber());
        assertEquals(BlockNotification.Type.BLOCK_VERIFIED, notification.type());
        assertEquals(blockHash, notification.blockHash());
    }

    @Test
    void testBlockNotificationEquality() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockNotification notification1 = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash);
        BlockNotification notification2 = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash);
        assertEquals(notification1, notification2);
    }

    @Test
    void testBlockNotificationInequality() {
        Bytes blockHash1 = Bytes.wrap(new byte[] {1, 2, 3});
        Bytes blockHash2 = Bytes.wrap(new byte[] {4, 5, 6});
        BlockNotification notification1 = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash1);
        BlockNotification notification2 =
                new BlockNotification(2L, BlockNotification.Type.BLOCK_FAILED_VERIFICATION, blockHash2);
        assertNotEquals(notification1, notification2);
    }

    @Test
    void testBlockNotificationHashCode() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockNotification notification1 = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash);
        BlockNotification notification2 = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash);
        assertEquals(notification1.hashCode(), notification2.hashCode());
    }

    @Test
    void testBlockNotificationToString() {
        Bytes blockHash = Bytes.wrap(new byte[] {1, 2, 3});
        BlockNotification notification = new BlockNotification(1L, BlockNotification.Type.BLOCK_VERIFIED, blockHash);
        String expected = "BlockNotification[blockNumber=1, type=BLOCK_VERIFIED, blockHash=" + blockHash + "]";
        assertEquals(expected, notification.toString());
    }
}
