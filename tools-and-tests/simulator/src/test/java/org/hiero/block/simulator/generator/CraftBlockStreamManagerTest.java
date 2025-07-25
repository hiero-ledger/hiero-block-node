// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * Verifies that the {@link CraftBlockStreamManager} constructor throws a
     * {@link NullPointerException} when passed a {@code null} configuration or
     * a {@code null} unordered stream config.
     */
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

    /**
     * Verifies that an {@link IllegalStateException} is thrown when both
     * unordered block generation and startup data are enabled simultaneously.
     * This configuration is invalid as they are mutually exclusive sources.
     */
    @Test
    void testUnorderedBlockGenerationWithActiveStartupData() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(startupDataMock.isEnabled()).thenReturn(true);
        assertThrows(
                IllegalStateException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    /**
     * Verifies that an {@link IllegalStateException} is thrown when
     * unordered stream is enabled with a non-zero scramble level,
     * but the available block set is empty.
     */
    @Test
    void testUnorderedBlockGenerationWithEmptyStreamList() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(3);
        assertThrows(
                IllegalStateException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    /**
     * Tests unordered block generation with a valid block set and a
     * non-zero scramble level. Verifies that the resulting block stream
     * has the same size but a different order from the input.
     */
    @Test
    void testUnorderedBlockGenerationWithScrambleLevelMoreThanZero()
            throws IOException, BlockSimulatorParsingException {
        Set<Long> availableBlockNumbers = Set.of(2L, 3L, 4L, 6L, 7L, 8L, 9L, 13L);

        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("2, 3, 4, 6, 7, 8, 9, 13");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(3);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);

        List<Long> expectedStreamBlockNumbers = getExpectedStreamBlockNumbers();

        assertEquals(availableBlockNumbers.size(), expectedStreamBlockNumbers.size());
        assertNotEquals(new ArrayList<>(availableBlockNumbers), expectedStreamBlockNumbers);
    }

    /**
     * Similar to {@link #testUnorderedBlockGenerationWithScrambleLevelMoreThanZero()},
     * this test validates scrambling behavior when scramble level is > 5.
     */
    @Test
    void testUnorderedBlockGenerationWithScrambleLevelMoreThanFive()
            throws IOException, BlockSimulatorParsingException {
        Set<Long> availableBlockNumbers = Set.of(2L, 3L, 4L, 6L, 7L, 8L, 9L, 13L);

        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("2, 3, 4, 6, 7, 8, 9, 13");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(7);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);

        List<Long> expectedStreamBlockNumbers = getExpectedStreamBlockNumbers();

        assertEquals(availableBlockNumbers.size(), expectedStreamBlockNumbers.size());
        assertNotEquals(new ArrayList<>(availableBlockNumbers), expectedStreamBlockNumbers);
    }

    /**
     * Verifies that when the scramble level is 0, the generated block stream
     * matches the exact order and content of the {@code fixedStreamingSequence}.
     */
    @Test
    void testUnorderedBlockGenerationWithScrambleLevelZero() throws IOException, BlockSimulatorParsingException {
        LinkedHashSet<Long> fixedStreamingSequence = new LinkedHashSet<>();
        fixedStreamingSequence.add(1L);
        fixedStreamingSequence.add(2L);
        fixedStreamingSequence.add(3L);
        fixedStreamingSequence.add(99L);
        fixedStreamingSequence.add(5L);
        fixedStreamingSequence.add(6L);
        fixedStreamingSequence.add(4L);
        fixedStreamingSequence.add(55L);

        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.fixedStreamingSequence()).thenReturn("1, 2, 3, 99, 5, 6, 4, 55");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(0);
        manager = new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock);

        List<Long> expectedStreamBlockNumbers = getExpectedStreamBlockNumbers();

        assertEquals(fixedStreamingSequence.size(), expectedStreamBlockNumbers.size());
        assertEquals(new ArrayList<>(fixedStreamingSequence), new ArrayList<>(expectedStreamBlockNumbers));
    }

    /**
     * Verifies that an IllegalArgumentException is thrown when the `availableBlocks`
     * configuration string contains an invalid range where the start of the range is greater
     * than the end (e.g., "[5-3]"). This is considered an invalid configuration and should be rejected.
     */
    @Test
    void testInvalidGroupInAvailableBlocksBeginningBiggerThanEnd() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("[5-3]");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(2);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    /**
     * Verifies that an IllegalArgumentException is thrown when the `availableBlocks`
     * configuration string contains a range that includes zero (e.g., "[0-5]").
     * Zero is not allowed as a valid block number and should trigger validation failure.
     */
    @Test
    void testInvalidGroupInAvailableBlocksContainingZero() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("[0-5]");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(2);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    /**
     * Verifies that an IllegalArgumentException is thrown when the `availableBlocks`
     * configuration contains a range where the start and end values are the same (e.g., "[5-5]").
     * This is an invalid case since ranges must have strictly increasing bounds.
     */
    @Test
    void testInvalidGroupInAvailableBlocksEqualBeginningAndEnding() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("[5-5]");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(2);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    /**
     * Verifies that an IllegalArgumentException is thrown when the `availableBlocks`
     * configuration string contains a single invalid block number "0".
     * Since block numbers must be positive and non-zero, this input is invalid.
     */
    @Test
    void testInvalidGroupInAvailableBlocksSingleZero() {
        Mockito.when(unorderedStreamConfigMock.enabled()).thenReturn(true);
        Mockito.when(unorderedStreamConfigMock.availableBlocks()).thenReturn("0");
        Mockito.when(unorderedStreamConfigMock.sequenceScrambleLevel()).thenReturn(2);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CraftBlockStreamManager(generatorConfigMock, startupDataMock, unorderedStreamConfigMock));
    }

    /**
     * Helper method to extract all block numbers from the generated stream.
     *
     * @return a list of block numbers in the order they appear in the stream
     * @throws IOException if an I/O error occurs while reading a block
     * @throws BlockSimulatorParsingException if a parsing error occurs
     */
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
