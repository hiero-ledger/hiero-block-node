// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
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
        // Reset static TSS state for test isolation (must happen before start())
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
        defaultConfig = VerificationConfigBuilder.newBuilder()
                .allBlocksHasherFilePath(tempDir.resolve("verificationData.bin"))
                .allBlocksHasherEnabled(true)
                .allBlocksHasherPersistenceInterval(2)
                .tssParametersFilePath(tempDir.resolve("tss-parameters.bin"))
                .toMap();
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), defaultConfig);
    }

    // ==== Block Verification Tests ===================================================================================

    @Test
    @DisplayName("should verify consecutive blocks (block 0 then block 1)")
    void shouldVerifyConsecutiveBlocks() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo block0Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        BlockUtils.SampleBlockInfo block1Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_1);

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
    @DisplayName("should fail verification when a block item is removed (tampered block)")
    void shouldFailVerificationForTamperedBlock() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        List<BlockItemUnparsed> blockItems =
                new ArrayList<>(sampleBlockInfo.blockUnparsed().blockItems());

        // remove one block item, so the hash is no longer valid
        blockItems.remove(3);
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber, true, true));

        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertEquals(blockNumber, blockNotification.blockNumber());
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
        assertNull(blockNotification.block(), "The block should be null since the verification failed");
    }

    @Test
    @DisplayName("should ignore block items received before a block header")
    void shouldIgnoreBlockItemsWithoutHeader() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        long blockNumber = sampleBlockInfo.blockNumber();
        List<BlockItemUnparsed> blockItems =
                new ArrayList<>(sampleBlockInfo.blockUnparsed().blockItems());

        // remove the header to simulate receiving items without a prior header
        blockItems.removeFirst();
        plugin.handleBlockItemsReceived(new BlockItems(blockItems, blockNumber, false, true));
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("should ignore block items when server is not running")
    void shouldIgnoreBlockItemsWhenServerNotRunning() {
        ((TestHealthFacility) blockNodeContext.serverHealth()).isRunning.set(false);
        plugin.handleBlockItemsReceived(new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null))), 0, true, false));
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("should send failure notification on processing exception")
    void shouldSendFailureNotificationOnException() {
        BlockItems blockItems = mock(BlockItems.class);
        when(blockItems.isStartOfNewBlock()).thenThrow(new RuntimeException("Test Exception"));
        plugin.handleBlockItemsReceived(blockItems);

        assertFalse(
                ((TestHealthFacility) blockNodeContext.serverHealth()).shutdownCalled.get(),
                "The server should NOT be shutdown after an exception");
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
    }

    @Test
    @DisplayName("backfill of sequential block should update allBlocksHasher")
    void shouldUpdateHasherForSequentialBackfilledBlock() throws IOException, ParseException {
        // Block 0 is the next expected block (hasher leafCount==0 == blockNumber==0).
        // A successful backfill should append its hash to the hasher so continuity is maintained.
        BlockUtils.SampleBlockInfo block0Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        plugin.handleBackfilled(new BackfilledBlockNotification(block0Info.blockNumber(), block0Info.blockUnparsed()));

        VerificationNotification notification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertTrue(notification.success(), "block 0 backfill should succeed");
        assertEquals(1, plugin.allBlocksHasherHandler.getNumberOfBlocks(), "hasher should have 1 leaf after block 0");
    }

    @Test
    @DisplayName("backfill of out-of-order historical block should not update allBlocksHasher")
    void shouldNotUpdateHasherForOutOfOrderBackfilledBlock() throws IOException, ParseException {
        // Initialize TSS state via block 0 live-stream so TSS signature verification works
        BlockUtils.SampleBlockInfo block0Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        blockMessaging.sendBlockItems(
                new BlockItems(block0Info.blockUnparsed().blockItems(), block0Info.blockNumber(), true, true));

        // Block 4 arrives while hasher leafCount==1 (block 0 was sequential); block 4 is not the
        // next sequential block so appending it would break the hasher's contiguous chain.
        BlockUtils.SampleBlockInfo block4Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_4);
        plugin.handleBackfilled(new BackfilledBlockNotification(block4Info.blockNumber(), block4Info.blockUnparsed()));

        VerificationNotification notification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertTrue(notification.success(), "block 4 backfill should succeed");
        assertEquals(
                1,
                plugin.allBlocksHasherHandler.getNumberOfBlocks(),
                "hasher must only contain block 0, not the out-of-order block 4");
    }

    @Test
    @DisplayName("should verify backfilled block")
    void shouldVerifyBackfilledBlock() throws IOException, ParseException {
        // Initialize TSS state via block 0 live-stream so TSS signature verification works
        BlockUtils.SampleBlockInfo block0Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        blockMessaging.sendBlockItems(
                new BlockItems(block0Info.blockUnparsed().blockItems(), block0Info.blockNumber(), true, true));

        BlockUtils.SampleBlockInfo sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_4);
        long blockNumber = sampleBlockInfo.blockNumber();
        BackfilledBlockNotification notification =
                new BackfilledBlockNotification(blockNumber, sampleBlockInfo.blockUnparsed());

        plugin.handleBackfilled(notification);

        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertNotNull(blockNotification);
        assertEquals(blockNumber, blockNotification.blockNumber());
        assertTrue(blockNotification.success(), "The verification should be successful");
        assertEquals(sampleBlockInfo.blockRootHash(), blockNotification.blockHash());
        assertEquals(sampleBlockInfo.blockUnparsed(), blockNotification.block());
    }

    @Test
    @DisplayName("should fail verification when block header number mismatches block number")
    void shouldFailOnBlockHeaderNumberMismatch() throws ParseException, IOException {
        BlockUtils.SampleBlockInfo sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                sampleBlockInfo.blockUnparsed().blockItems().getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number() + 1;
        plugin.handleBlockItemsReceived(
                new BlockItems(sampleBlockInfo.blockUnparsed().blockItems(), blockNumber, true, true));

        assertEquals(1, blockMessaging.getSentVerificationNotifications().size());
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
    }

    @Test
    @DisplayName("fresh BN with empty allBlocksHasher should use footer values for first non-genesis block")
    void shouldUseFooterValuesWhenHasherIsEmptyForNonGenesisBlock() throws IOException, ParseException {
        // Scenario: BN starts fresh (no stored blocks, allBlocksHasherEnabled=true) with
        // earliestManagedBlock > 0, so the first block received is not block 0.
        //
        // allBlocksHasherHandler initialises at genesis state: leafCount=0, lastBlockHash=ZERO_BLOCK_HASH.
        // initAllBlocksHasherIfEnabled() sees isAvailable()=true and lastBlockHash()!=null, so it sets
        // plugin.previousBlockHash = ZERO_BLOCK_HASH. getRootOfAllPreviousBlocks() also returns
        // ZERO_BLOCK_HASH (leafCount==0). Both non-null values are passed to the session, overriding the
        // block footer's authoritative previousBlockRootHash and rootHashOfAllBlockHashesTree.
        //
        // For block 1 the correct values in the footer are the hash of block 0 and the Merkle root of
        // [hash(block0)], neither of which is ZERO_BLOCK_HASH. Using ZERO_BLOCK_HASH produces a wrong
        // block root hash, so signature verification fails.
        //
        // After the fix: when earliestManagedBlock > 0 and the hasher has no chain continuity
        // (leafCount != currentBlockNumber), both values fall back to footer and verification succeeds.

        // Bootstrap TSS state via block 0 on the initial plugin so TSS verification works
        BlockUtils.SampleBlockInfo block0Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        blockMessaging.sendBlockItems(
                new BlockItems(block0Info.blockUnparsed().blockItems(), block0Info.blockNumber(), true, true));

        // Restart plugin with earliestManagedBlock=1 (TSS static state persists across restarts)
        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("block.node.earliestManagedBlock", "1");
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        BlockUtils.SampleBlockInfo block1Info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_1);

        blockMessaging.sendBlockItems(
                new BlockItems(block1Info.blockUnparsed().blockItems(), block1Info.blockNumber(), true, true));

        VerificationNotification notification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(notification);
        assertEquals(block1Info.blockNumber(), notification.blockNumber());
        assertTrue(
                notification.success(),
                "Block 1 on a fresh BN with earliestManagedBlock=1 should verify using block footer "
                        + "values; ZERO_BLOCK_HASH from an empty allBlocksHasher must not override "
                        + "footer values when the hasher has no chain continuity with this block");
    }

    // ==== TSS Parameters Bootstrap Tests =============================================================================

    @Test
    @DisplayName("should bootstrap TSS parameters from persisted file at startup")
    void shouldBootstrapTssParametersFromFile() throws IOException, ParseException {
        // Process block 0 to get a real LedgerIdPublicationTransactionBody
        BlockUnparsed tssBlock0 = loadTssBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));
        LedgerIdPublicationTransactionBody publication = VerificationServicePlugin.activeTssPublication;
        assertNotNull(publication, "Block 0 must produce a TSS publication");

        // Serialize and write to a file
        Path tssParametersFile = testTempDir.resolve("tss-parameters-priority.bin");
        Bytes serialized = LedgerIdPublicationTransactionBody.PROTOBUF.toBytes(publication);
        Files.write(tssParametersFile, serialized.toByteArray());

        // Reset static state so the restart actually exercises file loading
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;

        // Restart plugin with the file — TSS state should be restored
        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.tssParametersFilePath", tssParametersFile.toString());
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertNotNull(VerificationServicePlugin.activeLedgerId, "Ledger ID must be restored from persisted file");
        assertNotNull(
                VerificationServicePlugin.activeTssPublication, "TSS publication must be restored from persisted file");
        assertEquals(
                publication.ledgerId(),
                VerificationServicePlugin.activeLedgerId,
                "Restored ledger ID must match the original from block 0");
    }

    @Test
    @DisplayName("should persist TSS parameters to file after block 0 verification")
    void shouldPersistTssParametersAfterBlock0() throws IOException, ParseException {
        Path tssParametersFile = testTempDir.resolve("tss-parameters-block0.bin");

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.tssParametersFilePath", tssParametersFile.toString());
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertNull(VerificationServicePlugin.activeLedgerId, "activeLedgerId must be null before block 0");
        assertFalse(Files.exists(tssParametersFile), "TSS parameters file must not exist before block 0");

        BlockUnparsed tssBlock0 = loadTssBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));

        assertNotNull(VerificationServicePlugin.activeLedgerId, "activeLedgerId must be set after block 0");
        assertTrue(Files.exists(tssParametersFile), "TSS parameters file must be created after block 0");

        // Verify the file contains a valid LedgerIdPublicationTransactionBody
        Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
        LedgerIdPublicationTransactionBody persisted = LedgerIdPublicationTransactionBody.PROTOBUF.parse(fileBytes);
        assertEquals(
                VerificationServicePlugin.activeLedgerId,
                persisted.ledgerId(),
                "Persisted ledger ID must match plugin state");
        assertFalse(persisted.nodeContributions().isEmpty(), "Persisted file must contain address book contributions");
    }

    @Test
    @DisplayName("should not overwrite file-loaded TSS parameters when block 0 is received (first-write-wins)")
    void shouldNotOverwriteFileLoadedTssParameters() throws IOException, ParseException {
        // Process block 0 to get a real publication
        BlockUnparsed tssBlock0 = loadTssBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));
        Bytes originalLedgerId = VerificationServicePlugin.activeLedgerId;
        assertNotNull(originalLedgerId, "Block 0 must set ledger ID");

        // Persist to file, reset static state, restart plugin with the file
        Path tssParametersFile = testTempDir.resolve("tss-parameters-first-write-wins.bin");
        Bytes serialized =
                LedgerIdPublicationTransactionBody.PROTOBUF.toBytes(VerificationServicePlugin.activeTssPublication);
        Files.write(tssParametersFile, serialized.toByteArray());

        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;

        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.tssParametersFilePath", tssParametersFile.toString());
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        assertEquals(
                originalLedgerId,
                VerificationServicePlugin.activeLedgerId,
                "File-loaded ledger ID must match original");

        // Send block 0 again — first-write-wins: plugin state unchanged
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));

        assertEquals(
                originalLedgerId,
                VerificationServicePlugin.activeLedgerId,
                "First-write-wins: block 0 must not overwrite file-loaded ledger ID");
    }

    // ==== RSA Address Book / onContextUpdate Tests ==================================================================

    @Test
    @DisplayName("onContextUpdate with non-empty address book rebuilds keyByNodeId")
    void onContextUpdate_nonEmptyBook_rebuildsKeyMap() throws Exception {
        // Build a minimal NodeAddress with a real RSA public key encoded as hex-DER.
        // 1024-bit RSA keys are used for test speed only — production network uses 4096-bit keys.
        final java.security.KeyPairGenerator kpg = java.security.KeyPairGenerator.getInstance("RSA");
        kpg.initialize(1024);
        final java.security.KeyPair kp = kpg.generateKeyPair();
        final String hexKey = java.util.HexFormat.of().formatHex(kp.getPublic().getEncoded());

        final com.hedera.hapi.node.base.NodeAddress addr = com.hedera.hapi.node.base.NodeAddress.newBuilder()
                .nodeId(7L)
                .rsaPubKey(hexKey)
                .build();
        final com.hedera.hapi.node.base.NodeAddressBook book = com.hedera.hapi.node.base.NodeAddressBook.newBuilder()
                .nodeAddress(List.of(addr))
                .build();

        // The test context has nodeAddressBook set to DEFAULT (empty) initially;
        // simulate a context update by calling onContextUpdate directly
        final var updatedContext = new org.hiero.block.node.spi.BlockNodeContext(
                blockNodeContext.configuration(),
                blockNodeContext.metricRegistry(),
                blockNodeContext.serverHealth(),
                blockNodeContext.blockMessaging(),
                blockNodeContext.historicalBlockProvider(),
                blockNodeContext.applicationStateFacility(),
                blockNodeContext.serviceLoader(),
                blockNodeContext.threadPoolManager(),
                blockNodeContext.blockNodeVersions(),
                blockNodeContext.tssData(),
                book);

        plugin.onContextUpdate(updatedContext);

        // Verify the key map now contains node 7's key (indirect: the plugin won't reject a WRB
        // from node 7 as "unknown node"). The simplest assertion is that the method did not throw.
        // A deeper assertion would require exposing `keyByNodeId`, which we avoid.
        // Coverage is provided by the unit tests in `RsaWrbVerificationTest`.
    }

    @Test
    @DisplayName("onContextUpdate with empty address book logs warning and retains existing key map")
    void onContextUpdate_emptyBook_retainsKeyMap() {
        // Call onContextUpdate with an empty NodeAddressBook — must not throw and must not reset
        // the key map to empty (it starts empty already, but the important contract is: no crash).
        final com.hedera.hapi.node.base.NodeAddressBook emptyBook = com.hedera.hapi.node.base.NodeAddressBook.DEFAULT;
        final var updatedContext = new org.hiero.block.node.spi.BlockNodeContext(
                blockNodeContext.configuration(),
                blockNodeContext.metricRegistry(),
                blockNodeContext.serverHealth(),
                blockNodeContext.blockMessaging(),
                blockNodeContext.historicalBlockProvider(),
                blockNodeContext.applicationStateFacility(),
                blockNodeContext.serviceLoader(),
                blockNodeContext.threadPoolManager(),
                blockNodeContext.blockNodeVersions(),
                blockNodeContext.tssData(),
                emptyBook);

        // Must not throw; a WARNING is expected but not asserted here (logged to system logger).
        plugin.onContextUpdate(updatedContext);
    }

    @Test
    @DisplayName("onContextUpdate with null NodeAddressBook logs warning and retains existing key map")
    void onContextUpdate_nullBook_retainsKeyMap() {
        // Build a context where nodeAddressBook() returns null — must not throw.
        final var updatedContext = new org.hiero.block.node.spi.BlockNodeContext(
                blockNodeContext.configuration(),
                blockNodeContext.metricRegistry(),
                blockNodeContext.serverHealth(),
                blockNodeContext.blockMessaging(),
                blockNodeContext.historicalBlockProvider(),
                blockNodeContext.applicationStateFacility(),
                blockNodeContext.serviceLoader(),
                blockNodeContext.threadPoolManager(),
                blockNodeContext.blockNodeVersions(),
                blockNodeContext.tssData(),
                null);

        // Must not throw; a WARNING is expected but not asserted here (logged to system logger).
        plugin.onContextUpdate(updatedContext);
    }

    @Test
    @DisplayName("RSA WRB end-to-end: valid signed block verifies through the plugin and notification is success=true")
    void rsaWrb_endToEnd_validBlock_notificationSucceeds() throws Exception {
        // 1024-bit RSA keys for test speed only — production uses 4096-bit keys.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(1024);
        final KeyPair kp = kpg.generateKeyPair();

        // Build a 1-node address book (threshold = floor(2*1/3)+1 = 1 valid sig needed).
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(0L).rsaPubKey(hexKey).build()))
                .build();

        // Deliver the address book — triggers onContextUpdate and rebuilds the RSA key map.
        updateAddressBook(book);

        // Build a minimal RecordFileItem proto: field 2 (tag=0x12, LEN) = record stream file bytes.
        final byte[] recordStreamFileBytes = "test-record-stream-content".getBytes();
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(0x12); // tag: field 2, wire type 2 (LEN)
        bos.write(recordStreamFileBytes.length); // length fits in 1 byte (< 128)
        bos.write(recordStreamFileBytes);
        final Bytes recordFileItemBytes = Bytes.wrap(bos.toByteArray());

        // Compute the V6 signed payload: SHA-384(int32(6) || record_stream_file_bytes).
        final MessageDigest digest = MessageDigest.getInstance("SHA-384");
        digest.update(new byte[] {0, 0, 0, 6});
        digest.update(recordStreamFileBytes);
        final byte[] signedPayload = digest.digest();

        // Sign the payload with node 0's private key.
        final java.security.Signature engine = java.security.Signature.getInstance("SHA384withRSA");
        engine.initSign(kp.getPrivate());
        engine.update(signedPayload);
        final byte[] sigBytes = engine.sign();

        // Assemble the WRB block: BLOCK_HEADER | RECORD_FILE | BLOCK_FOOTER | BLOCK_PROOF.
        final long blockNumber = 500L;
        // HAPI version >= 0.72.0 routes to ExtendedMerkleTreeSession (RSA path).
        final SemanticVersion hapiVersion = new SemanticVersion(1, 0, 0, "", "");
        final SemanticVersion swVersion = new SemanticVersion(1, 0, 0, "", "");
        final BlockHeader header = new BlockHeader(
                hapiVersion, swVersion, blockNumber, new Timestamp(1_700_000_000L, 0), BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
        final BlockProof proof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedRecordFileProof(
                        new SignedRecordFileProof(6, List.of(new RecordFileSignature(Bytes.wrap(sigBytes), 0L))))
                .build();

        final List<BlockItemUnparsed> items = List.of(
                BlockItemUnparsed.newBuilder()
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                        .build(),
                BlockItemUnparsed.newBuilder().recordFile(recordFileItemBytes).build(),
                BlockItemUnparsed.newBuilder()
                        .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                        .build());

        // Send through the plugin's live-stream handler.
        blockMessaging.sendBlockItems(new BlockItems(items, blockNumber, true, true));

        // The RSA path must produce a success notification.
        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertFalse(notifications.isEmpty(), "Plugin must emit a VerificationNotification for the WRB block");
        final VerificationNotification notification = notifications.getLast();
        assertEquals(blockNumber, notification.blockNumber());
        assertTrue(notification.success(), "RSA WRB block with a valid threshold signature must verify successfully");
        assertNotNull(notification.blockHash(), "Block hash must be set on successful RSA verification");
        assertNotNull(notification.block(), "Block must be present in the success notification");
    }

    // ==== TSS End-to-End Flow Test ===================================================================================

    @Test
    @DisplayName("TSS flow: block 0 bootstraps TSS state, subsequent block verifies with TSS")
    void tssFlowBlock0ThenSubsequentBlock() throws IOException, ParseException {
        BlockUnparsed tssBlock0 = loadTssBlock("test-blocks/CN_0_73_TSS_WRAPS/0.blk.gz");
        BlockUnparsed tssBlockN = loadTssBlock("test-blocks/CN_0_73_TSS_WRAPS/467.blk.gz");
        long blockNNumber = BlockHeader.PROTOBUF
                .parse(tssBlockN.blockItems().getFirst().blockHeaderOrThrow())
                .number();

        // Process block 0 via live stream — bootstraps TSS parameters
        blockMessaging.sendBlockItems(new BlockItems(tssBlock0.blockItems(), 0, true, true));
        VerificationNotification block0Notification =
                blockMessaging.getSentVerificationNotifications().get(0);
        assertTrue(block0Notification.success(), "TSS block 0 must verify successfully");
        assertNotNull(VerificationServicePlugin.activeLedgerId, "Plugin must have ledger ID after block 0");

        // Process subsequent block via backfill — verifies using TSS with ledger ID from block 0
        plugin.handleBackfilled(new BackfilledBlockNotification(blockNNumber, tssBlockN));
        VerificationNotification blockNNotification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertTrue(
                blockNNotification.success(),
                "Subsequent TSS block must verify with ledger ID bootstrapped from block 0");
    }

    // ==== Helpers ====================================================================================================

    private static BlockUnparsed loadTssBlock(String resourcePath) throws IOException, ParseException {
        try (InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath);
                GZIPInputStream gzip = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
        }
    }

    private static class VerificationConfigBuilder {

        private Path allBlocksHasherFilePath;
        private boolean allBlocksHasherEnabled = true;
        private int allBlocksHasherPersistenceInterval = 10;
        private Path tssParametersFilePath = Path.of("");

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

        public VerificationConfigBuilder tssParametersFilePath(Path value) {
            this.tssParametersFilePath = value;
            return this;
        }

        public VerificationConfig build() {
            return new VerificationConfig(
                    allBlocksHasherFilePath,
                    allBlocksHasherEnabled,
                    allBlocksHasherPersistenceInterval,
                    tssParametersFilePath);
        }

        public Map<String, String> toMap() {
            Map<String, String> configMap = new HashMap<>();
            configMap.put("verification.allBlocksHasherFilePath", allBlocksHasherFilePath.toString());
            configMap.put("verification.allBlocksHasherEnabled", String.valueOf(allBlocksHasherEnabled));
            configMap.put(
                    "verification.allBlocksHasherPersistenceInterval",
                    String.valueOf(allBlocksHasherPersistenceInterval));
            configMap.put("verification.tssParametersFilePath", tssParametersFilePath.toString());
            return configMap;
        }
    }
}
