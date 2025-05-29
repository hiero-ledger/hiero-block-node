// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

    private final byte[] previousBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
    private final byte[] currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
    private final long blockNumber = 1L;

    @Test
    void testConstructorWithNullPreviousHash() {
        assertThrows(NullPointerException.class, () -> new BlockProofHandler(null, currentBlockHash, blockNumber));
    }

    @Test
    void testConstructorWithNullCurrentHash() {
        assertThrows(NullPointerException.class, () -> new BlockProofHandler(previousBlockHash, null, blockNumber));
    }

    @Test
    void testGetItem() {
        BlockProofHandler handler = new BlockProofHandler(previousBlockHash, currentBlockHash, blockNumber);
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasBlockProof());

        BlockProof proof = item.getBlockProof();
        assertEquals(blockNumber, proof.getBlock());
        assertArrayEquals(previousBlockHash, proof.getPreviousBlockRootHash().toByteArray());
        assertNotNull(proof.getBlockSignature());
        assertFalse(proof.getBlockSignature().isEmpty());
    }

    @Test
    void testGetItemCaching() {
        BlockProofHandler handler = new BlockProofHandler(previousBlockHash, currentBlockHash, blockNumber);
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }
}
