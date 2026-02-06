// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.hiero.block.tools.blocks.NoOpAmendmentProvider;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Tests that wrapped blocks produced from real mainnet record data can be validated successfully,
 * and unit tests for individual validation methods in {@link WrappedBlockValidator}.
 *
 * <p>Uses the test resource {@code 2019-09-13.tar.zstd} (first day of mainnet) as source data.
 * Converts record file blocks to wrapped format using {@link RecordBlockConverter} and writes them
 * using {@link BlockWriter} (simulating {@link org.hiero.block.tools.blocks.ToWrappedBlocksCommand}),
 * then validates with {@link ValidateWrappedBlocksCommand} invoked via picocli.
 */
class ValidateWrappedBlocksCommandTest {

    /** Minimal BlockHeader item for structure tests. */
    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder().number(0).build())
            .build();

    /** Minimal RecordFile item for structure tests. */
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();

    /** Minimal BlockFooter item for structure tests. */
    private static final BlockItem FOOTER_ITEM = BlockItem.newBuilder()
            .blockFooter(com.hedera.hapi.block.stream.output.BlockFooter.newBuilder()
                    .previousBlockRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .rootHashOfAllBlockHashesTree(Bytes.wrap(EMPTY_TREE_HASH))
                    .startOfBlockStateRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .build())
            .build();

    /** Minimal BlockProof item for structure tests. */
    private static final BlockItem PROOF_ITEM =
            BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();

    /** Minimal StateChanges item for structure tests. */
    private static final BlockItem STATE_CHANGES_ITEM = BlockItem.newBuilder()
            .stateChanges(StateChanges.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(1L).build())
                    .build())
            .build();

    /** A valid minimal block: [BlockHeader, RecordFile, BlockFooter, BlockProof]. */
    private static final Block VALID_BLOCK = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));

    @TempDir
    Path tempDir;

    // ===== End-to-end test =====

    /**
     * End-to-end test: convert first-day mainnet record blocks to wrapped format, write to disk,
     * then invoke {@link ValidateWrappedBlocksCommand} via picocli to validate all blocks.
     */
    @Test
    void testConvertAndValidateFirstDayBlocks() throws Exception {
        assumeTrue(isZstdAvailable(), "Skipping test: zstd command not available");

        final Path tarZstdPath =
                Path.of(Objects.requireNonNull(getClass().getResource("/2019-09-13.tar.zstd")).toURI());

        // ===== Phase 1: Convert record blocks to wrapped blocks (simulates ToWrappedBlocksCommand) =====

        final List<UnparsedRecordBlock> unparsedBlocks = TarZstdDayReaderUsingExec.readTarZstd(tarZstdPath);
        assertFalse(unparsedBlocks.isEmpty(), "Should have record blocks in the tar.zstd");

        final NodeAddressBook addressBook = new AddressBookRegistry().getCurrentAddressBook();
        final NoOpAmendmentProvider amendmentProvider = new NoOpAmendmentProvider();
        final StreamingHasher conversionHasher = new StreamingHasher();
        byte[] previousBlockHash = EMPTY_TREE_HASH;

        long blockCount = 0;
        for (UnparsedRecordBlock unparsedBlock : unparsedBlocks) {
            final var parsedBlock = unparsedBlock.parse();

            final Block wrappedBlock = RecordBlockConverter.toBlock(
                    parsedBlock,
                    blockCount,
                    previousBlockHash,
                    conversionHasher.computeRootHash(),
                    addressBook,
                    amendmentProvider);

            BlockWriter.writeBlock(tempDir, wrappedBlock, BlockArchiveType.INDIVIDUAL_FILES);

            final byte[] blockHash = hashBlock(wrappedBlock);
            conversionHasher.addNodeByHash(blockHash);
            previousBlockHash = blockHash;

            blockCount++;
        }

        assertTrue(blockCount > 0, "Should have processed at least one block");

        // ===== Phase 2: Validate using ValidateWrappedBlocksCommand via picocli =====
        // The 50 billion HBAR supply check will fail because NoOpAmendmentProvider does not
        // include genesis state changes with initial account balances. Once wrapping is fixed
        // to include all amendments, change this assertion to expect exit code 0.

        int exitCode = new CommandLine(new ValidateWrappedBlocksCommand()).execute(tempDir.toString(), "-n", "none");
        assertEquals(1, exitCode, "Validation should fail: 50 billion check requires genesis amendments");
    }

    // ===== validateRequiredItems tests =====

    @Test
    void validateRequiredItems_validBlock_passes() {
        assertDoesNotThrow(() -> WrappedBlockValidator.validateRequiredItems(0, VALID_BLOCK));
    }

    @Test
    void validateRequiredItems_emptyBlock_fails() {
        Block emptyBlock = new Block(List.of());
        ValidationException ex = assertThrows(
                ValidationException.class, () -> WrappedBlockValidator.validateRequiredItems(0, emptyBlock));
        assertTrue(ex.getMessage().contains("no items"));
    }

    @Test
    void validateRequiredItems_missingHeader_fails() {
        Block block = new Block(List.of(RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateRequiredItems(0, block));
        assertTrue(ex.getMessage().contains("BlockHeader"));
    }

    @Test
    void validateRequiredItems_missingRecordFile_fails() {
        Block block = new Block(List.of(HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateRequiredItems(0, block));
        assertTrue(ex.getMessage().contains("RecordFile"));
    }

    @Test
    void validateRequiredItems_missingFooter_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateRequiredItems(0, block));
        assertTrue(ex.getMessage().contains("BlockFooter"));
    }

    @Test
    void validateRequiredItems_missingProof_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateRequiredItems(0, block));
        assertTrue(ex.getMessage().contains("BlockProof"));
    }

    // ===== validate50Billion tests =====

    @Test
    void validate50Billion_nullMap_skipsValidation() {
        assertDoesNotThrow(() -> WrappedBlockValidator.validate50Billion(0, VALID_BLOCK, null));
    }

    // ===== validateNoExtraItems tests =====

    @Test
    void validateNoExtraItems_validBlock_passes() {
        assertDoesNotThrow(() -> WrappedBlockValidator.validateNoExtraItems(0, VALID_BLOCK));
    }

    @Test
    void validateNoExtraItems_withStateChanges_passes() {
        Block block = new Block(List.of(
                HEADER_ITEM, STATE_CHANGES_ITEM, STATE_CHANGES_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> WrappedBlockValidator.validateNoExtraItems(0, block));
    }

    @Test
    void validateNoExtraItems_multipleProofs_passes() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> WrappedBlockValidator.validateNoExtraItems(0, block));
    }

    @Test
    void validateNoExtraItems_withStateChangesAndMultipleProofs_passes() {
        Block block = new Block(List.of(
                HEADER_ITEM, STATE_CHANGES_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM, PROOF_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> WrappedBlockValidator.validateNoExtraItems(0, block));
    }

    @Test
    void validateNoExtraItems_duplicateHeader_fails() {
        Block block = new Block(List.of(HEADER_ITEM, HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("Multiple BlockHeaders"));
    }

    @Test
    void validateNoExtraItems_duplicateRecordFile_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("Multiple RecordFile"));
    }

    @Test
    void validateNoExtraItems_duplicateFooter_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("Multiple BlockFooter"));
    }

    @Test
    void validateNoExtraItems_stateChangesAfterRecordFile_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, STATE_CHANGES_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("Expected BlockFooter"));
    }

    @Test
    void validateNoExtraItems_headerNotFirst_fails() {
        Block block = new Block(List.of(RECORD_FILE_ITEM, HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("First item must be a BlockHeader"));
    }

    @Test
    void validateNoExtraItems_unexpectedItemAfterProofs_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM, RECORD_FILE_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("Unexpected"));
    }

    @Test
    void validateNoExtraItems_missingProof_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        ValidationException ex =
                assertThrows(ValidationException.class, () -> WrappedBlockValidator.validateNoExtraItems(0, block));
        assertTrue(ex.getMessage().contains("Expected BlockProof"));
    }

    // ===== Helpers =====

    private static boolean isZstdAvailable() {
        try {
            Process p = new ProcessBuilder("which", "zstd").start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
