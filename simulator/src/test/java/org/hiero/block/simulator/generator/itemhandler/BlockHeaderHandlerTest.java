// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hederahashgraph.api.proto.java.BlockHashAlgorithm;
import org.junit.jupiter.api.Test;

class BlockHeaderHandlerTest {

    private final byte[] previousBlockHash = new byte[32];
    private final long blockNumber = 1L;

    @Test
    void testConstructorWithNullHash() {
        assertThrows(NullPointerException.class, () -> new BlockHeaderHandler(null, blockNumber));
    }

    @Test
    void testGetItem() {
        BlockHeaderHandler handler = new BlockHeaderHandler(previousBlockHash, blockNumber);
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasBlockHeader());

        BlockHeader header = item.getBlockHeader();
        assertEquals(BlockHashAlgorithm.SHA2_384, header.getHashAlgorithm());
        assertEquals(blockNumber, header.getNumber());
        assertNotNull(header.getFirstTransactionConsensusTime());
        assertNotNull(header.getHapiProtoVersion());
        assertNotNull(header.getSoftwareVersion());
    }

    @Test
    void testGetItemCaching() {
        BlockHeaderHandler handler = new BlockHeaderHandler(previousBlockHash, blockNumber);
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }

    @Test
    void testSemanticVersions() {
        BlockHeaderHandler handler = new BlockHeaderHandler(previousBlockHash, blockNumber);
        BlockHeader header = handler.getItem().getBlockHeader();

        assertEquals(0, header.getHapiProtoVersion().getMajor());
        assertEquals(1, header.getHapiProtoVersion().getMinor());
        assertEquals(0, header.getHapiProtoVersion().getPatch());

        assertEquals(0, header.getSoftwareVersion().getMajor());
        assertEquals(1, header.getSoftwareVersion().getMinor());
        assertEquals(0, header.getSoftwareVersion().getPatch());
    }
}
