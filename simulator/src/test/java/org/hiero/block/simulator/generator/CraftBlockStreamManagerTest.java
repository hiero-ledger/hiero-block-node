// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CraftBlockStreamManagerTest {
    private BlockGeneratorConfig mockConfig;
    private CraftBlockStreamManager manager;

    @BeforeEach
    void setUp() {
        mockConfig = Mockito.mock(BlockGeneratorConfig.class);
        Mockito.when(mockConfig.generationMode()).thenReturn(GenerationMode.CRAFT);
        Mockito.when(mockConfig.startBlockNumber()).thenReturn(1);
        Mockito.when(mockConfig.minEventsPerBlock()).thenReturn(1);
        Mockito.when(mockConfig.maxEventsPerBlock()).thenReturn(3);
        Mockito.when(mockConfig.minTransactionsPerEvent()).thenReturn(1);
        Mockito.when(mockConfig.maxTransactionsPerEvent()).thenReturn(2);

        manager = new CraftBlockStreamManager(mockConfig);
    }

    @Test
    void testConstructorNullConfig() {
        assertThrows(NullPointerException.class, () -> new CraftBlockStreamManager(null));
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
        Block block = manager.getNextBlock();
        assertNotNull(block);
        assertTrue(block.getItemsCount() > 0);

        // Each block should have at least:
        // 1 block header + 1 event header + 1 transaction + 1 result + 1 proof
        assertTrue(block.getItemsCount() >= 5);
    }

    @Test
    void testMultipleBlockGeneration() throws IOException, BlockSimulatorParsingException {
        Block block1 = manager.getNextBlock();
        Block block2 = manager.getNextBlock();

        assertNotNull(block1);
        assertNotNull(block2);
        assertNotEquals(0, block1.getItemsCount());
        assertNotEquals(0, block2.getItemsCount());
    }

    @Test
    void testBlockGenerationWithCustomValues() throws IOException, BlockSimulatorParsingException {
        Mockito.when(mockConfig.minEventsPerBlock()).thenReturn(3);
        Mockito.when(mockConfig.maxEventsPerBlock()).thenReturn(4);
        Mockito.when(mockConfig.minTransactionsPerEvent()).thenReturn(2);
        Mockito.when(mockConfig.maxTransactionsPerEvent()).thenReturn(3);

        manager = new CraftBlockStreamManager(mockConfig);
        Block block = manager.getNextBlock();

        // We expect at least:
        // 1 block header + (3 events * (1 header + 2 transactions * 2 items)) + 1 proof = 17
        assertTrue(block.getItemsCount() >= 17);
    }
}
