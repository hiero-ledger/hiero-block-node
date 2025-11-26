// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.junit.jupiter.api.Test;

class BlockProofHandlerTest {

    private final byte[] currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
    private final long blockNumber = 1L;

    @Test
    void testConstructorWithNullHash() {
        assertThrows(NullPointerException.class, () -> new BlockProofHandler(null, blockNumber));
    }

    @Test
    void testConstructorWithNullCurrentHash() {
        assertThrows(NullPointerException.class, () -> new BlockProofHandler(null, blockNumber));
    }

    @Test
    void testGetItem() {
        BlockProofHandler handler = new BlockProofHandler(currentBlockHash, blockNumber);
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasBlockProof());

        BlockProof proof = item.getBlockProof();
        assertEquals(blockNumber, proof.getBlock());
        assertNotNull(proof.getSignedBlockProof());
        assertFalse(proof.getSignedBlockProof().getBlockSignature().isEmpty());
    }

    @Test
    void testGetItemCaching() {
        BlockProofHandler handler = new BlockProofHandler(currentBlockHash, blockNumber);
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }
}
