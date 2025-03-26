// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CraftBlockStreamManagerTest {
    private static final int START_BLOCK_NUMBER = 1;
    private BlockGeneratorConfig generatorConfigMock;
    private SimulatorStartupData startupDataMock;
    private CraftBlockStreamManager manager;

    @BeforeEach
    void setUp() {
        generatorConfigMock = Mockito.mock(BlockGeneratorConfig.class);
        Mockito.when(generatorConfigMock.generationMode()).thenReturn(GenerationMode.CRAFT);
        Mockito.when(generatorConfigMock.startBlockNumber()).thenReturn(START_BLOCK_NUMBER);
        Mockito.when(generatorConfigMock.minEventsPerBlock()).thenReturn(1);
        Mockito.when(generatorConfigMock.maxEventsPerBlock()).thenReturn(3);
        Mockito.when(generatorConfigMock.minTransactionsPerEvent()).thenReturn(1);
        Mockito.when(generatorConfigMock.maxTransactionsPerEvent()).thenReturn(2);
        startupDataMock = Mockito.mock(SimulatorStartupData.class);
        Mockito.when(startupDataMock.getLatestAckBlockNumber()).thenReturn((long) START_BLOCK_NUMBER);
        Mockito.when(startupDataMock.getLatestAckBlockHash()).thenReturn(new byte[StreamingTreeHasher.HASH_LENGTH]);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock);
    }

    @Test
    void testConstructorNullConfig() {
        assertThrows(NullPointerException.class, () -> new CraftBlockStreamManager(null, startupDataMock));
    }

    @Test
    void testGetGenerationMode() {
        assertEquals(GenerationMode.CRAFT, manager.getGenerationMode());
    }

    @Test
    void testGetNextBlockItem() {
        assertThrows(UnsupportedOperationException.class, () -> manager.getNextBlockItem());
    }

    @Test
    void testGetNextBlock() throws IOException, BlockSimulatorParsingException {
        final Block block = manager.getNextBlock();
        assertNotNull(block);
        assertTrue(block.getItemsCount() > 0);
        // Each block should have at least:
        // 1 block header + 1 event header + 1 transaction + 1 result + 1 proof
        assertTrue(block.getItemsCount() >= 5);
    }

    @Test
    void testMultipleBlockGeneration() throws IOException, BlockSimulatorParsingException {
        final Block block1 = manager.getNextBlock();
        final Block block2 = manager.getNextBlock();
        assertNotNull(block1);
        assertNotNull(block2);
        assertNotEquals(0, block1.getItemsCount());
        assertNotEquals(0, block2.getItemsCount());
    }

    @Test
    void testBlockGenerationWithCustomValues() throws IOException, BlockSimulatorParsingException {
        Mockito.when(generatorConfigMock.minEventsPerBlock()).thenReturn(3);
        Mockito.when(generatorConfigMock.maxEventsPerBlock()).thenReturn(4);
        Mockito.when(generatorConfigMock.minTransactionsPerEvent()).thenReturn(2);
        Mockito.when(generatorConfigMock.maxTransactionsPerEvent()).thenReturn(3);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock);
        final Block block = manager.getNextBlock();
        // We expect at least:
        // 1 block header + (3 events * (1 header + 2 transactions * 2 items)) + 1 proof = 17
        assertTrue(block.getItemsCount() >= 17);
    }
}
