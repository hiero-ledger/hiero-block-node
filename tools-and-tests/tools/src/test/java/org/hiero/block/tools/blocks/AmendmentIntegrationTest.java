// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;
import org.hiero.block.tools.records.model.parsed.ParsedRecordFile;
import org.hiero.block.tools.records.model.parsed.ParsedSignatureFile;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests for amendment functionality using real mainnet data.
 * Uses the 2019-09-13.tar.zstd test resource which contains block 0 (genesis).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AmendmentIntegrationTest {

    private static final String TEST_ARCHIVE = "/2019-09-13.tar.zstd";
    private static final long GENESIS_BLOCK_NUMBER = 0L;

    /** Dummy hashes for block conversion */
    private static final byte[] DUMMY_PREVIOUS_HASH = new byte[48];

    private static final byte[] DUMMY_ROOT_HASH = new byte[48];

    private ParsedRecordBlock block0;
    private AddressBookRegistry addressBookRegistry;

    @BeforeAll
    void setUp() throws Exception {
        // Verify test resource exists
        URL resource = getClass().getResource(TEST_ARCHIVE);
        assertNotNull(resource, "Test resource " + TEST_ARCHIVE + " must exist");

        // Load block 0 from the archive
        block0 = loadFirstBlockFromArchive();
        assertNotNull(block0, "Should load block 0 from archive");

        // Initialize address book registry
        addressBookRegistry = new AddressBookRegistry();
    }

    @Nested
    @DisplayName("Genesis amendments for block 0")
    class GenesisAmendmentsTests {

        @Test
        @DisplayName("MainnetAmendmentProvider should have genesis amendments for block 0")
        void testMainnetProviderHasGenesisAmendmentsForBlock0() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();

            assertTrue(provider.hasGenesisAmendments(GENESIS_BLOCK_NUMBER), "Block 0 should have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(1L), "Block 1 should not have genesis amendments");
        }

        @Test
        @DisplayName("Block 0 converted with MainnetAmendmentProvider should include STATE_CHANGES")
        void testBlock0IncludesStateChanges() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();

            Block block = RecordBlockConverter.toBlock(
                    block0,
                    GENESIS_BLOCK_NUMBER,
                    DUMMY_PREVIOUS_HASH,
                    DUMMY_ROOT_HASH,
                    addressBookRegistry.getCurrentAddressBook(),
                    provider);

            assertNotNull(block, "Converted block should not be null");

            // Block structure should be: [HEADER, STATE_CHANGES..., RECORD_FILE, FOOTER, PROOF]
            // Count STATE_CHANGES items
            long stateChangesCount =
                    block.items().stream().filter(BlockItem::hasStateChanges).count();

            assertTrue(stateChangesCount > 0, "Block 0 should have STATE_CHANGES items from genesis amendments");

            // Verify structure order
            assertTrue(block.items().get(0).hasBlockHeader(), "First item should be BLOCK_HEADER");
            assertTrue(block.items().get(1).hasStateChanges(), "Second item should be STATE_CHANGES (genesis)");

            // Find RECORD_FILE position
            int recordFileIndex = -1;
            for (int i = 0; i < block.items().size(); i++) {
                if (block.items().get(i).hasRecordFile()) {
                    recordFileIndex = i;
                    break;
                }
            }
            assertTrue(recordFileIndex > 1, "RECORD_FILE should come after BLOCK_HEADER and STATE_CHANGES");
        }

        @Test
        @DisplayName("Block 0 converted without amendments should not have STATE_CHANGES")
        void testBlock0WithoutAmendmentsHasNoStateChanges() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();

            Block block = RecordBlockConverter.toBlock(
                    block0,
                    GENESIS_BLOCK_NUMBER,
                    DUMMY_PREVIOUS_HASH,
                    DUMMY_ROOT_HASH,
                    addressBookRegistry.getCurrentAddressBook(),
                    provider);

            // Count STATE_CHANGES items
            long stateChangesCount =
                    block.items().stream().filter(BlockItem::hasStateChanges).count();

            assertEquals(0, stateChangesCount, "Block 0 with NoOpAmendmentProvider should have no STATE_CHANGES");

            // Structure should be: [HEADER, RECORD_FILE, FOOTER, PROOF]
            assertEquals(4, block.items().size(), "Block should have exactly 4 items without amendments");
        }
    }

    @Nested
    @DisplayName("Block structure validation")
    class BlockStructureTests {

        @Test
        @DisplayName("Converted block has correct item order")
        void testBlockItemOrder() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();

            Block block = RecordBlockConverter.toBlock(
                    block0,
                    GENESIS_BLOCK_NUMBER,
                    DUMMY_PREVIOUS_HASH,
                    DUMMY_ROOT_HASH,
                    addressBookRegistry.getCurrentAddressBook(),
                    provider);

            // Verify order: HEADER first, PROOF last
            assertTrue(block.items().getFirst().hasBlockHeader(), "First item must be BLOCK_HEADER");
            assertTrue(block.items().getLast().hasBlockProof(), "Last item must be BLOCK_PROOF");

            // FOOTER should be second-to-last
            assertTrue(
                    block.items().get(block.items().size() - 2).hasBlockFooter(),
                    "Second-to-last item must be BLOCK_FOOTER");
        }

        @Test
        @DisplayName("Block header has correct block number")
        void testBlockHeaderNumber() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();

            Block block = RecordBlockConverter.toBlock(
                    block0,
                    GENESIS_BLOCK_NUMBER,
                    DUMMY_PREVIOUS_HASH,
                    DUMMY_ROOT_HASH,
                    addressBookRegistry.getCurrentAddressBook(),
                    provider);

            long blockNumber = block.items().getFirst().blockHeaderOrThrow().number();
            assertEquals(GENESIS_BLOCK_NUMBER, blockNumber, "Block header should have block number 0");
        }
    }

    // ========== Helper Methods ==========

    /**
     * Loads the first block from the test archive.
     */
    private ParsedRecordBlock loadFirstBlockFromArchive() throws Exception {
        try (InputStream is = getClass().getResourceAsStream(TEST_ARCHIVE);
                com.github.luben.zstd.ZstdInputStream zstdIn = new com.github.luben.zstd.ZstdInputStream(is);
                TarArchiveInputStream tarIn = new TarArchiveInputStream(zstdIn)) {

            ParsedRecordFile recordFile = null;
            java.util.List<ParsedSignatureFile> signatureFiles = new java.util.ArrayList<>();

            TarArchiveEntry entry;
            while ((entry = tarIn.getNextEntry()) != null) {
                if (entry.isDirectory()) continue;

                String name = entry.getName();
                byte[] content = tarIn.readAllBytes();

                if (name.endsWith(".rcd")) {
                    // Record file
                    InMemoryFile imf = new InMemoryFile(Path.of(name), content);
                    recordFile = ParsedRecordFile.parse(imf);
                } else if (name.endsWith(".rcd_sig")) {
                    // Signature file - parse later once we have the record file
                    // For now, just collect them
                }

                // Stop after first block (first .rcd file)
                if (recordFile != null) {
                    break;
                }
            }

            if (recordFile == null) {
                throw new IllegalStateException("No record file found in archive");
            }

            // For simplicity, return with empty signature files (validation not needed for these tests)
            return new ParsedRecordBlock(recordFile, signatureFiles, java.util.Collections.emptyList());
        }
    }
}
