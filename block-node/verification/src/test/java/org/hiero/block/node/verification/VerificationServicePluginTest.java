// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.impl.ExtendedMerkleTreeSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Plugin-level integration test for {@link VerificationServicePlugin}.
 *
 * <p>All test blocks used here must use the latest supported HAPI version that routes to a real
 * verification session (i.e. {@code ExtendedMerkleTreeSession}). Never use synthetic or older HAPI
 * version blocks that route to {@code DummyVerificationSession}, as that bypasses the actual
 * verification logic this test is meant to exercise.
 */
class VerificationServicePluginTest
        extends PluginTestBase<VerificationServicePlugin, BlockingExecutor, ScheduledExecutorService> {

    Path testTempDir;

    Map<String, String> defaultConfig;

    public VerificationServicePluginTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);
        defaultConfig = VerificationConfigBuilder.newBuilder()
                .allBlocksHasherFilePath(tempDir.resolve("verificationData.bin"))
                .allBlocksHasherEnabled(true)
                .allBlocksHasherPersistenceInterval(2)
                .ledgerIdFilePath(tempDir.resolve("ledger-id.bin"))
                .toMap();
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), defaultConfig);
    }

    @BeforeEach
    void resetLedgerState() {
        // Reset process-level TSS state to ensure test isolation between test instances.
        ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.set(null);
    }

    @Test
    void testVerificationPlugin() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo block0Info =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_0);
        BlockUtils.SampleBlockInfo block1Info =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_1);

        blockMessaging.sendBlockItems(
                new BlockItems(block0Info.blockUnparsed().blockItems(), block0Info.blockNumber(), true, true));
        blockMessaging.sendBlockItems(
                new BlockItems(block1Info.blockUnparsed().blockItems(), block1Info.blockNumber(), true, true));

        // check block 0 verification
        VerificationNotification block0Notification =
                blockMessaging.getSentVerificationNotifications().get(0);
        assertNotNull(block0Notification);
        assertEquals(block0Info.blockNumber(), block0Notification.blockNumber(), "block 0 number should match");
        assertTrue(block0Notification.success(), "block 0 verification should succeed");
        assertEquals(block0Info.blockRootHash(), block0Notification.blockHash(), "block 0 hash should match");
        assertEquals(block0Info.blockUnparsed(), block0Notification.block(), "block 0 content should match");

        // check block 1 verification
        VerificationNotification block1Notification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertNotNull(block1Notification);
        assertEquals(block1Info.blockNumber(), block1Notification.blockNumber(), "block 1 number should match");
        assertTrue(block1Notification.success(), "block 1 verification should succeed");
        assertEquals(block1Info.blockRootHash(), block1Notification.blockHash(), "block 1 hash should match");
        assertEquals(block1Info.blockUnparsed(), block1Notification.block(), "block 1 content should match");
    }

    @Test
    void testFailedVerification() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_21);
        // get the original block items
        List<BlockItemUnparsed> originalItems = sampleBlockInfo.blockUnparsed().blockItems();
        // make a mutable copy
        List<BlockItemUnparsed> blockItems = new ArrayList<>(originalItems);

        // remove one block item, so the hash is no longer valid
        blockItems.remove(3);
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber, true, true));

        // check we received a block verification
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);

        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
        assertNotEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
        assertNull(blockNotification.block(), "The block should be null since the verification failed");
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived without a block header")
    void testHandleBlockItemsReceived_NoCurrentSession() throws IOException, ParseException {
        // create sample block data
        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_0);
        long blockNumber = sampleBlockInfo.blockNumber();
        List<BlockItemUnparsed> originalItems = sampleBlockInfo.blockUnparsed().blockItems();

        // make a mutable copy
        List<BlockItemUnparsed> blockItems = new ArrayList<>(originalItems);

        // remove the header to simulate a case where receive items and have never received a header
        blockItems.removeFirst();
        // send some items to the plugin, they should be ignored
        plugin.handleBlockItemsReceived(new BlockItems(blockItems, blockNumber, false, true));
        // check we did not receive a block verification
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived with non-running server")
    void testHandleBlockItemsReceived_NotRunning() {
        // make the server state not running
        ((TestHealthFacility) blockNodeContext.serverHealth()).isRunning.set(false);
        // send some items to the plugin, they should be ignored
        plugin.handleBlockItemsReceived(new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null))), 0, true, false));
        // check we did not receive a block verification
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived with BlockItems that throws an exception")
    void testHandleBlockItemsReceived_ExceptionThrown() {
        // mock a BlockItems object to throw an exception when isStartOfNewBlock is called
        BlockItems blockItems = mock(BlockItems.class);
        when(blockItems.isStartOfNewBlock()).thenThrow(new RuntimeException("Test Exception"));
        // sent the mocked BlockItems to the plugin
        plugin.handleBlockItemsReceived(blockItems);
        // check the exception was thrown and resulted in a shutdown
        assertFalse(
                ((TestHealthFacility) blockNodeContext.serverHealth()).shutdownCalled.get(),
                "The server should NOT be shutdown after an exception is thrown on VerificationServicePlugin");

        // check we get a failed verification notification
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
    }

    @Test
    @DisplayName("Test handleBackfilled with a valid backfilled block")
    void testHandleBackfilledNotification() throws IOException, ParseException {

        // prepare test data
        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_21);

        long blockNumber = sampleBlockInfo.blockNumber();
        BackfilledBlockNotification notification =
                new BackfilledBlockNotification(blockNumber, sampleBlockInfo.blockUnparsed());

        // call the method with a valid backfilled block notification
        plugin.handleBackfilled(notification);

        // check we received a block verification notification
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");
        assertTrue(blockNotification.success(), "The verification should be successful");
        assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
        assertEquals(
                sampleBlockInfo.blockUnparsed(),
                blockNotification.block(),
                "The block should be the same as the one sent");
    }

    @Test
    @DisplayName("BlockHeader number and blockNumber on constructor mismatch, should fail but not throw")
    void blockHeaderAndNumberMismatch() throws ParseException, IOException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_0);

        BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                sampleBlockInfo.blockUnparsed().blockItems().getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number() + 1;
        plugin.handleBlockItemsReceived(
                new BlockItems(sampleBlockInfo.blockUnparsed().blockItems(), blockNumber, true, true));

        // check we don't received a block verification notification
        long blockNotifications =
                blockMessaging.getSentVerificationNotifications().size();
        assertEquals(1, blockNotifications);
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
    }

    // ==== Ledger ID Bootstrap Path Tests =============================================================================

    @Test
    @DisplayName("Persisted file takes priority over configured ledger ID string at startup")
    void fileBootstrapsLedgerId() throws IOException {
        byte[] fileBytes = new byte[] {1, 2, 3, 4, 5, 6};
        Path ledgerIdFile = testTempDir.resolve("ledger-id-priority.bin");
        Files.write(ledgerIdFile, fileBytes);

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.ledgerIdFilePath", ledgerIdFile.toString());
        config.put("verification.ledgerId", "aabbccdd"); // should be ignored
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertArrayEquals(
                fileBytes,
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get().toByteArray(),
                "File-persisted ledger ID must take priority over the configured string");
    }

    @Test
    @DisplayName("Config string pre-seeds ACTIVE_LEDGER_ID when no persisted file exists")
    void configStringPreSeededLedgerId() throws IOException {
        String ledgerIdHex = "deadbeef";
        Path ledgerIdFile = testTempDir.resolve("ledger-id-config-no-file.bin");

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.ledgerIdFilePath", ledgerIdFile.toString());
        config.put("verification.ledgerId", ledgerIdHex);
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertEquals(
                Bytes.fromHex(ledgerIdHex),
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get(),
                "Config string must seed ACTIVE_LEDGER_ID when no persisted file exists");
        assertFalse(Files.exists(ledgerIdFile), "Config string must NOT create the ledger ID file");
    }

    @Test
    @DisplayName("Processing block 0 persists ledger ID to file")
    void block0PersistsLedgerId() throws IOException, ParseException {
        Path ledgerIdFile = testTempDir.resolve("ledger-id-block0.bin");

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.ledgerIdFilePath", ledgerIdFile.toString());
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertNull(ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get(), "ACTIVE_LEDGER_ID must be null before block 0");
        assertFalse(Files.exists(ledgerIdFile), "Ledger ID file must not exist before block 0");

        BlockUnparsed tssBlock0 = loadTssBlock0();
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));

        assertNotNull(ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get(), "ACTIVE_LEDGER_ID must be set after block 0");
        assertTrue(Files.exists(ledgerIdFile), "Ledger ID file must be created after block 0");
        assertArrayEquals(
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get().toByteArray(),
                Files.readAllBytes(ledgerIdFile),
                "File content must match ACTIVE_LEDGER_ID set from block 0");
    }

    @Test
    @DisplayName("Block 0 overwrites config-string ledger ID and writes authoritative value to file")
    void block0OverwritesConfigString() throws IOException, ParseException {
        String sentinelHex = "aabb1234";
        Path ledgerIdFile = testTempDir.resolve("ledger-id-overwrite-config.bin");

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.ledgerIdFilePath", ledgerIdFile.toString());
        config.put("verification.ledgerId", sentinelHex);
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        Bytes sentinelBytes = Bytes.fromHex(sentinelHex);
        assertEquals(
                sentinelBytes,
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get(),
                "Config string seeds ACTIVE_LEDGER_ID before block 0");
        assertFalse(Files.exists(ledgerIdFile), "Config string must not create ledger ID file");

        BlockUnparsed tssBlock0 = loadTssBlock0();
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));

        assertNotEquals(
                sentinelBytes,
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get(),
                "Block 0 must overwrite the config-string ledger ID");
        assertTrue(Files.exists(ledgerIdFile), "Block 0 must write the ledger ID file");
        assertArrayEquals(
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get().toByteArray(),
                Files.readAllBytes(ledgerIdFile),
                "File must contain the block 0 ledger ID, not the config-string sentinel");
    }

    @Test
    @DisplayName("Block 0 overwrites file-loaded ledger ID in memory and updates the file")
    void block0UpdatesExistingFile() throws IOException, ParseException {
        byte[] sentinelBytes = new byte[] {9, 8, 7, 6, 5, 4};
        Path ledgerIdFile = testTempDir.resolve("ledger-id-update-file.bin");
        Files.write(ledgerIdFile, sentinelBytes);

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.ledgerIdFilePath", ledgerIdFile.toString());
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertArrayEquals(
                sentinelBytes,
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get().toByteArray(),
                "Startup must load ledger ID from existing file");

        BlockUnparsed tssBlock0 = loadTssBlock0();
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));

        assertFalse(
                java.util.Arrays.equals(
                        sentinelBytes,
                        ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get().toByteArray()),
                "Block 0 must overwrite the file-loaded ledger ID in memory");
        assertArrayEquals(
                ExtendedMerkleTreeSession.ACTIVE_LEDGER_ID.get().toByteArray(),
                Files.readAllBytes(ledgerIdFile),
                "File must be updated with the block 0 ledger ID");
    }

    // ==== Helpers ====================================================================================================

    private static BlockUnparsed loadTssBlock0() throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream("test-blocks/tss/TssWraps/0.blk.gz");
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }

    private static class VerificationConfigBuilder {

        // Fields with default values
        private Path allBlocksHasherFilePath;
        private boolean allBlocksHasherEnabled = true;
        private int allBlocksHasherPersistenceInterval = 10;
        private String ledgerId = "";
        private Path ledgerIdFilePath = Path.of("");

        public static VerificationConfigBuilder newBuilder() {
            return new VerificationConfigBuilder();
        }

        public VerificationConfigBuilder allBlocksHasherEnabled(boolean value) {
            this.allBlocksHasherEnabled = value;
            return this;
        }

        public VerificationConfigBuilder allBlocksHasherFilePath(Path value) {
            this.allBlocksHasherFilePath = value;
            return this;
        }

        public VerificationConfigBuilder allBlocksHasherPersistenceInterval(int value) {
            this.allBlocksHasherPersistenceInterval = value;
            return this;
        }

        public VerificationConfigBuilder ledgerId(String value) {
            this.ledgerId = value;
            return this;
        }

        public VerificationConfigBuilder ledgerIdFilePath(Path value) {
            this.ledgerIdFilePath = value;
            return this;
        }

        public VerificationConfig build() {
            return new VerificationConfig(
                    allBlocksHasherFilePath,
                    allBlocksHasherEnabled,
                    allBlocksHasherPersistenceInterval,
                    ledgerId,
                    ledgerIdFilePath);
        }

        public Map<String, String> toMap() {
            Map<String, String> configMap = new HashMap<>();
            configMap.put("verification.allBlocksHasherFilePath", allBlocksHasherFilePath.toString());
            configMap.put("verification.allBlocksHasherEnabled", String.valueOf(allBlocksHasherEnabled));
            configMap.put(
                    "verification.allBlocksHasherPersistenceInterval",
                    String.valueOf(allBlocksHasherPersistenceInterval));
            configMap.put("verification.ledgerId", ledgerId);
            configMap.put("verification.ledgerIdFilePath", ledgerIdFilePath.toString());
            return configMap;
        }
    }
}
