// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CraftBlockStreamManagerTest {
    private static final int START_BLOCK_NUMBER = 1;
    private BlockGeneratorConfig generatorConfigMock;
    private UnorderedStreamConfig unorderedStreamConfigMock;
    private SimulatorStartupData startupDataMock;
    private CraftBlockStreamManager manager;

    @BeforeEach
    void setUp() {
        generatorConfigMock = Mockito.mock(BlockGeneratorConfig.class);
        unorderedStreamConfigMock = Mockito.mock(UnorderedStreamConfig.class);
        Mockito.when(generatorConfigMock.generationMode()).thenReturn(GenerationMode.CRAFT);
        Mockito.when(generatorConfigMock.startBlockNumber()).thenReturn(START_BLOCK_NUMBER);
        Mockito.when(generatorConfigMock.minEventsPerBlock()).thenReturn(1);
        Mockito.when(generatorConfigMock.maxEventsPerBlock()).thenReturn(3);
        Mockito.when(generatorConfigMock.minTransactionsPerEvent()).thenReturn(1);
        Mockito.when(generatorConfigMock.maxTransactionsPerEvent()).thenReturn(2);
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(false);
        startupDataMock = Mockito.mock(SimulatorStartupData.class);
        Mockito.when(startupDataMock.getLatestAckBlockNumber()).thenReturn((long) START_BLOCK_NUMBER);
        Mockito.when(startupDataMock.getLatestAckBlockHash()).thenReturn(new byte[StreamingTreeHasher.HASH_LENGTH]);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);
    }

    @Test
    void testConstructorNullConfig() {
        assertThrows(
                NullPointerException.class,
                () -> new CraftBlockStreamManager(null, startupDataMock, unorderedStreamConfigMock));
        assertThrows(
                NullPointerException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, null));
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
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);
        final Block block = manager.getNextBlock();
        // We expect at least:
        // 1 block header + (3 events * (1 header + 2 transactions * 2 items)) + 1 proof = 17
        assertTrue(block.getItemsCount() >= 17);
    }

    @Test
    void testUnorderedBlockGenerationWithActiveStartupData() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(startupDataMock.isEnabled()).thenReturn(true);
        assertThrows(
                IllegalStateException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    @Test
    void testUnorderedBlockGenerationWithEmptyStreamList() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocksAsSet()).thenReturn(new HashSet<>());
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(3);
        assertThrows(
                IllegalStateException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    @Test
    void testUnorderedBlockGenerationWithScrambleLevelMoreThanZero()
            throws IOException, BlockSimulatorParsingException {
        Set<Long> availableBlockNumbers = Set.of(2L, 3L, 4L, 6L, 7L, 8L, 9L, 13L);

        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocksAsSet()).thenReturn(availableBlockNumbers);
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(3);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);

        // When sequenceScrambleLevel is set to more than 0,
        // sequence in the stream should differ from the increasing order in availableBlockNumbers
        // The amount of blocks in the scrambled stream must be the same as the amount of available blocks
        List<Long> expectedStreamBlockNumbers = getExpectedStreamBlockNumbers();

        assertEquals(availableBlockNumbers.size(), expectedStreamBlockNumbers.size());
        // ArrayList to compare by order and not just by content
        assertNotEquals(new ArrayList<>(availableBlockNumbers), expectedStreamBlockNumbers);
    }

    @Test
    void testUnorderedBlockGenerationWithScrambleLevelMoreThanFive()
            throws IOException, BlockSimulatorParsingException {
        Set<Long> availableBlockNumbers = Set.of(2L, 3L, 4L, 6L, 7L, 8L, 9L, 13L);
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocksAsSet()).thenReturn(availableBlockNumbers);
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(7);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);
        List<Long> expectedStreamBlockNumbers = getExpectedStreamBlockNumbers();
        assertEquals(availableBlockNumbers.size(), expectedStreamBlockNumbers.size());
        assertNotEquals(new ArrayList<>(availableBlockNumbers), expectedStreamBlockNumbers);
    }

    @Test
    void testUnorderedBlockGenerationWithScrambleLevelZero() throws IOException, BlockSimulatorParsingException {
        LinkedHashSet<Long> fixedStreamingSequence = new LinkedHashSet<>(Set.of(1L, 2L, 3L, 99L, 5L, 6L, 4L, 55L));

        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.fixedStreamingSequenceAsSet()).thenReturn(fixedStreamingSequence);
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(0);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);

        // When sequenceScrambleLevel is set to 0,
        // sequence in the stream should match the fixedStreamingSequence set
        // The amount of blocks in the scrambled stream must be the same as the size of fixedStreamingSequence
        List<Long> expectedStreamBlockNumbers = getExpectedStreamBlockNumbers();

        assertEquals(fixedStreamingSequence.size(), expectedStreamBlockNumbers.size());
        // arraylist to compare both order and content
        assertEquals(new ArrayList<>(fixedStreamingSequence), new ArrayList<>(expectedStreamBlockNumbers));
    }

    private List<Long> getExpectedStreamBlockNumbers() throws IOException, BlockSimulatorParsingException {
        List<Long> expectedBlockNumbers = new ArrayList<>();
        while (true) {
            Block block = manager.getNextBlock();
            if (Objects.nonNull(block)) {
                long blockNbr = block.getItems(block.getItemsCount() - 1)
                        .getBlockProof()
                        .getBlock();
                expectedBlockNumbers.add(blockNbr);
            } else {
                break;
            }
        }
        return expectedBlockNumbers;
    }
}
