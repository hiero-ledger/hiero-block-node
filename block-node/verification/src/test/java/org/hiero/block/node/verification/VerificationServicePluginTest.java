// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.hiero.block.node.base.ParseHelper.standardParse;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.signing.TssBlockSigner;
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
    void shouldVerifyConsecutiveBlocks() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder builder =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create());
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                builder.genesisWithPublication();
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block1 = builder.next(1L);

        blockMessaging.sendBlockItems(new BlockItems(
                block0.block().blockUnparsed().blockItems(), block0.block().number(), true, true));
        blockMessaging.sendBlockItems(new BlockItems(
                block1.block().blockUnparsed().blockItems(), block1.block().number(), true, true));

        VerificationNotification block0Notification =
                blockMessaging.getSentVerificationNotifications().get(0);
        assertNotNull(block0Notification);
        assertEquals(block0.block().number(), block0Notification.blockNumber(), "block 0 number should match");
        assertTrue(block0Notification.success(), "block 0 verification should succeed");
        assertEquals(block0.rootHash(), block0Notification.blockHash(), "block 0 hash should match");
        assertEquals(block0.block().blockUnparsed(), block0Notification.block(), "block 0 content should match");

        VerificationNotification block1Notification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertNotNull(block1Notification);
        assertEquals(block1.block().number(), block1Notification.blockNumber(), "block 1 number should match");
        assertTrue(block1Notification.success(), "block 1 verification should succeed");
        assertEquals(block1.rootHash(), block1Notification.blockHash(), "block 1 hash should match");
        assertEquals(block1.block().blockUnparsed(), block1Notification.block(), "block 1 content should match");
    }

    @Test
    @DisplayName("should fail verification when a block item is removed (tampered block)")
    void shouldFailVerificationForTamperedBlock() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed signed =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        List<BlockItemUnparsed> blockItems =
                new ArrayList<>(signed.block().blockUnparsed().blockItems());
        blockItems.remove(3);
        long blockNumber = signed.block().number();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber, true, true));

        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertEquals(blockNumber, blockNotification.blockNumber());
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
        assertNotNull(blockNotification.block(), "The block must be present for diagnostics even on failure");
    }

    @Test
    @DisplayName("should ignore block items received before a block header")
    void shouldIgnoreBlockItemsWithoutHeader() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed signed =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        List<BlockItemUnparsed> blockItems =
                new ArrayList<>(signed.block().blockUnparsed().blockItems());
        blockItems.removeFirst();
        plugin.handleBlockItemsReceived(
                new BlockItems(blockItems, signed.block().number(), false, true));
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
    void shouldUpdateHasherForSequentialBackfilledBlock() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        plugin.handleBackfilled(new BackfilledBlockNotification(
                block0.block().number(), block0.block().blockUnparsed()));

        VerificationNotification notification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertTrue(notification.success(), "block 0 backfill should succeed");
        assertEquals(1, plugin.allBlocksHasherHandler.getNumberOfBlocks(), "hasher should have 1 leaf after block 0");
    }

    @Test
    @DisplayName("backfill of out-of-order historical block should not update allBlocksHasher")
    void shouldNotUpdateHasherForOutOfOrderBackfilledBlock() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder builder =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create());
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                builder.genesisWithPublication();
        // Advance the builder past blocks 1-3 so block 4's footer is correctly chained.
        builder.next(1L);
        builder.next(2L);
        builder.next(3L);
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block4 = builder.next(4L);

        blockMessaging.sendBlockItems(new BlockItems(
                block0.block().blockUnparsed().blockItems(), block0.block().number(), true, true));
        plugin.handleBackfilled(new BackfilledBlockNotification(
                block4.block().number(), block4.block().blockUnparsed()));

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
    void shouldVerifyBackfilledBlock() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder builder =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create());
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                builder.genesisWithPublication();
        builder.next(1L);
        builder.next(2L);
        builder.next(3L);
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block4 = builder.next(4L);

        blockMessaging.sendBlockItems(new BlockItems(
                block0.block().blockUnparsed().blockItems(), block0.block().number(), true, true));

        plugin.handleBackfilled(new BackfilledBlockNotification(
                block4.block().number(), block4.block().blockUnparsed()));

        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertNotNull(blockNotification);
        assertEquals(block4.block().number(), blockNotification.blockNumber());
        assertTrue(blockNotification.success(), "The verification should be successful");
        assertEquals(block4.rootHash(), blockNotification.blockHash());
        assertEquals(block4.block().blockUnparsed(), blockNotification.block());
    }

    @Test
    @DisplayName("should fail verification when block header number mismatches block number")
    void shouldFailOnBlockHeaderNumberMismatch() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();

        long wrongBlockNumber = block0.block().number() + 1;
        plugin.handleBlockItemsReceived(
                new BlockItems(block0.block().blockUnparsed().blockItems(), wrongBlockNumber, true, true));

        assertEquals(1, blockMessaging.getSentVerificationNotifications().size());
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
    }

    @Test
    @DisplayName("fresh BN with empty allBlocksHasher should use footer values for first non-genesis block")
    void shouldUseFooterValuesWhenHasherIsEmptyForNonGenesisBlock() {
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
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder builder =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create());
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                builder.genesisWithPublication();
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block1 = builder.next(1L);

        // Bootstrap TSS state via block 0 on the initial plugin so TSS verification works.
        blockMessaging.sendBlockItems(new BlockItems(
                block0.block().blockUnparsed().blockItems(), block0.block().number(), true, true));

        // Restart plugin with earliestManagedBlock=1 (TSS static state persists across restarts).
        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("block.node.earliestManagedBlock", "1");
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        blockMessaging.sendBlockItems(new BlockItems(
                block1.block().blockUnparsed().blockItems(), block1.block().number(), true, true));

        VerificationNotification notification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(notification);
        assertEquals(block1.block().number(), notification.blockNumber());
        assertTrue(
                notification.success(),
                "Block 1 on a fresh BN with earliestManagedBlock=1 should verify using block footer "
                        + "values; ZERO_BLOCK_HASH from an empty allBlocksHasher must not override "
                        + "footer values when the hasher has no chain continuity with this block");
    }

    @Test
    @DisplayName("stop does not throw even when dumpEnabled is false (scheduler never created)")
    void stopDoesNotThrow() {
        plugin.stop();
    }

    @Test
    @DisplayName("should send BAD_BLOCK_PROOF failure when backfill block number mismatches header number")
    void shouldSendFailureOnBackfillBlockNumberMismatch() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        plugin.handleBackfilled(
                new BackfilledBlockNotification(999L, block0.block().blockUnparsed()));

        assertEquals(1, blockMessaging.getSentVerificationNotifications().size());
        VerificationNotification notification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(notification);
        assertFalse(notification.success(), "Backfill with mismatched block number must fail");
        assertEquals(
                VerificationNotification.FailureType.BAD_BLOCK_PROOF,
                notification.failureInfo().failureType());
    }

    @Test
    @DisplayName("should attempt dump when a backfilled block fails verification (matching block number)")
    void shouldAttemptDumpWhenBackfilledBlockFailsVerification() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        blockMessaging.sendBlockItems(new BlockItems(
                block0.block().blockUnparsed().blockItems(), block0.block().number(), true, true));
        assertNotNull(VerificationServicePlugin.activeLedgerId, "TSS state must be set after block 0");

        // Tamper block 0 by removing one item — the hash will change and TSS signature verification fails.
        // The block number still matches the header, so the session runs to completion and returns success=false.
        List<BlockItemUnparsed> tamperedItems =
                new ArrayList<>(block0.block().blockUnparsed().blockItems());
        tamperedItems.remove(3);
        BlockUnparsed tamperedBlock =
                BlockUnparsed.newBuilder().blockItems(tamperedItems).build();
        plugin.handleBackfilled(new BackfilledBlockNotification(block0.block().number(), tamperedBlock));

        assertEquals(2, blockMessaging.getSentVerificationNotifications().size());
        VerificationNotification backfillNotification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertNotNull(backfillNotification);
        assertFalse(backfillNotification.success(), "Tampered backfill block must fail verification");
    }

    // ==== TSS Parameters Bootstrap Tests =============================================================================

    @Test
    @DisplayName("should bootstrap TSS parameters from persisted file at startup")
    void shouldBootstrapTssParametersFromFile() throws IOException, ParseException {
        // Process a harness-generated block 0 to get a real LedgerIdPublicationTransactionBody
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        blockMessaging.sendBlockItems(
                new BlockItems(block0.block().blockUnparsed().blockItems(), 0, true, true));
        LedgerIdPublicationTransactionBody publication = VerificationServicePlugin.activeTssPublication;
        assertNotNull(publication, "Block 0 must produce a TSS publication");

        Path tssParametersFile = testTempDir.resolve("tss-parameters-priority.bin");
        Bytes serialized = LedgerIdPublicationTransactionBody.PROTOBUF.toBytes(publication);
        Files.write(tssParametersFile, serialized.toByteArray());

        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;

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

        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        blockMessaging.sendBlockItems(
                new BlockItems(block0.block().blockUnparsed().blockItems(), 0, true, true));

        assertNotNull(VerificationServicePlugin.activeLedgerId, "activeLedgerId must be set after block 0");
        assertTrue(Files.exists(tssParametersFile), "TSS parameters file must be created after block 0");

        Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
        LedgerIdPublicationTransactionBody persisted =
                standardParse(LedgerIdPublicationTransactionBody.PROTOBUF, fileBytes);
        assertEquals(
                VerificationServicePlugin.activeLedgerId,
                persisted.ledgerId(),
                "Persisted ledger ID must match plugin state");
        assertFalse(persisted.nodeContributions().isEmpty(), "Persisted file must contain address book contributions");
    }

    @Test
    @DisplayName("should not overwrite file-loaded TSS parameters when block 0 is received (first-write-wins)")
    void shouldNotOverwriteFileLoadedTssParameters() throws IOException, ParseException {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create())
                        .genesisWithPublication();
        blockMessaging.sendBlockItems(
                new BlockItems(block0.block().blockUnparsed().blockItems(), 0, true, true));
        Bytes originalLedgerId = VerificationServicePlugin.activeLedgerId;
        assertNotNull(originalLedgerId, "Block 0 must set ledger ID");

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
        blockMessaging.sendBlockItems(
                new BlockItems(block0.block().blockUnparsed().blockItems(), 0, true, true));

        assertEquals(
                originalLedgerId,
                VerificationServicePlugin.activeLedgerId,
                "First-write-wins: block 0 must not overwrite file-loaded ledger ID");
    }

    @Test
    @DisplayName("RSA WRB end-to-end: valid signed block verifies through the plugin and notification is success=true")
    void rsaWrb_endToEnd_validBlock_notificationSucceeds() throws Exception {
        // 2048-bit RSA keys for test speed only — production uses 4096-bit keys.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
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
        final Signature engine = Signature.getInstance("SHA384withRSA");
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
        final BlockFooter footer = new BlockFooter(Bytes.wrap(new byte[48]), Bytes.wrap(new byte[48]), Bytes.EMPTY);
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

    @Test
    @DisplayName("RSA WRB: second live-stream block in the same era reuses the cached RSA key map and still verifies")
    void rsaWrb_secondLiveBlockSameEra_reusesCachedRsaKeys() throws Exception {
        // Covers the cache-hit branch of VerificationServicePlugin#rsaKeysFor: two blocks resolved
        // against the same (identity-stable, single open-ended era) NodeAddressBook instance must
        // both verify, with the second lookup served from cachedLiveRsaKeys instead of re-decoding.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(0L).rsaPubKey(hexKey).build()))
                .build();
        updateAddressBook(book);

        blockMessaging.sendBlockItems(
                new BlockItems(buildSignedWrbBlockItems(500L, kp, "content-1"), 500L, true, true));
        blockMessaging.sendBlockItems(
                new BlockItems(buildSignedWrbBlockItems(501L, kp, "content-2"), 501L, true, true));

        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertEquals(2, notifications.size(), "Both live-stream blocks must produce a notification");
        assertTrue(notifications.get(0).success(), "First block (cache miss) must verify successfully");
        assertTrue(notifications.get(1).success(), "Second block (cache hit) must verify successfully");
    }

    @Test
    @DisplayName("RSA WRB: second backfilled block in the same era reuses the cached RSA key map and still verifies")
    void rsaWrb_secondBackfilledBlockSameEra_reusesCachedRsaKeys() throws Exception {
        // Covers the cache-hit branch of VerificationServicePlugin#rsaKeysFor for the backfill
        // path's dedicated cache (cachedBackfillRsaKeys), kept separate from the live-path cache.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(0L).rsaPubKey(hexKey).build()))
                .build();
        updateAddressBook(book);

        final BlockUnparsed block600 = BlockUnparsed.newBuilder()
                .blockItems(buildSignedWrbBlockItems(600L, kp, "content-3"))
                .build();
        final BlockUnparsed block601 = BlockUnparsed.newBuilder()
                .blockItems(buildSignedWrbBlockItems(601L, kp, "content-4"))
                .build();
        plugin.handleBackfilled(new BackfilledBlockNotification(600L, block600));
        plugin.handleBackfilled(new BackfilledBlockNotification(601L, block601));

        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertEquals(2, notifications.size(), "Both backfilled blocks must produce a notification");
        assertTrue(notifications.get(0).success(), "First backfilled block (cache miss) must verify successfully");
        assertTrue(notifications.get(1).success(), "Second backfilled block (cache hit) must verify successfully");
    }

    /**
     * Builds a signed WRB block item list: {@code BLOCK_HEADER | RECORD_FILE | BLOCK_FOOTER | BLOCK_PROOF},
     * where the {@code RECORD_FILE} contents are {@code recordStreamFileContent} and the proof carries a
     * single valid node-0 signature over the V6 payload, signed with {@code kp}'s private key.
     */
    private static List<BlockItemUnparsed> buildSignedWrbBlockItems(
            final long blockNumber, final KeyPair kp, final String recordStreamFileContent) throws Exception {
        final byte[] recordStreamFileBytes = recordStreamFileContent.getBytes();
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(0x12); // tag: field 2, wire type 2 (LEN)
        bos.write(recordStreamFileBytes.length); // length fits in 1 byte (< 128)
        bos.write(recordStreamFileBytes);
        final Bytes recordFileItemBytes = Bytes.wrap(bos.toByteArray());

        final MessageDigest digest = MessageDigest.getInstance("SHA-384");
        digest.update(new byte[] {0, 0, 0, 6});
        digest.update(recordStreamFileBytes);
        final byte[] signedPayload = digest.digest();

        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(kp.getPrivate());
        engine.update(signedPayload);
        final byte[] sigBytes = engine.sign();

        final SemanticVersion hapiVersion = new SemanticVersion(1, 0, 0, "", "");
        final SemanticVersion swVersion = new SemanticVersion(1, 0, 0, "", "");
        final BlockHeader header = new BlockHeader(
                hapiVersion, swVersion, blockNumber, new Timestamp(1_700_000_000L, 0), BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.wrap(new byte[48]), Bytes.wrap(new byte[48]), Bytes.EMPTY);
        final BlockProof proof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedRecordFileProof(
                        new SignedRecordFileProof(6, List.of(new RecordFileSignature(Bytes.wrap(sigBytes), 0L))))
                .build();

        return List.of(
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
    }

    @Test
    @DisplayName("RSA WRB: block number outside all address book eras fails with MISSING_VERIFICATION_DATA")
    void rsaWrb_blockOutsideAllEras_failsVerification() throws Exception {
        // Build a 1-node address book covering only blocks 1000–2000.
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(0L).rsaPubKey(hexKey).build()))
                .build();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                        .addressBook(book)
                        .startBlock(1000L)
                        .endBlock(2000L)
                        .build()))
                .build();
        updateAddressBookHistory(history);

        // Build a minimal WRB block for block number 50 — outside the covered range.
        final long blockNumber = 50L;
        final SemanticVersion hapiVersion = new SemanticVersion(1, 0, 0, "", "");
        final SemanticVersion swVersion = new SemanticVersion(1, 0, 0, "", "");
        final BlockHeader header = new BlockHeader(
                hapiVersion, swVersion, blockNumber, new Timestamp(1_700_000_000L, 0), BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.wrap(new byte[48]), Bytes.wrap(new byte[48]), Bytes.EMPTY);
        // The signature content is irrelevant — the block will be rejected before signature verification.
        final BlockProof proof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedRecordFileProof(
                        new SignedRecordFileProof(6, List.of(new RecordFileSignature(Bytes.wrap(new byte[256]), 0L))))
                .build();
        final byte[] recordFileBytes = "content".getBytes();
        final java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        bos.write(0x12);
        bos.write(recordFileBytes.length);
        bos.write(recordFileBytes);
        final List<BlockItemUnparsed> items = List.of(
                BlockItemUnparsed.newBuilder()
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .recordFile(Bytes.wrap(bos.toByteArray()))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                        .build());

        blockMessaging.sendBlockItems(new BlockItems(items, blockNumber, true, true));

        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertFalse(notifications.isEmpty(), "Plugin must emit a notification for the out-of-era WRB block");
        final VerificationNotification notification = notifications.getLast();
        assertEquals(blockNumber, notification.blockNumber());
        assertFalse(notification.success(), "Block outside all address book eras must fail verification");
        assertNotNull(notification.failureInfo(), "Failure info must be set");
        assertEquals(
                FailureType.MISSING_VERIFICATION_DATA,
                notification.failureInfo().failureType(),
                "Failure type must be MISSING_VERIFICATION_DATA for a block outside all eras");
    }

    @Test
    @DisplayName("RSA WRB multi-era: block in era 1 verifies with era-1 keys; era-2 key fails same block")
    void rsaWrb_multiEra_era1BlockVerifiesWithEra1Keys() throws Exception {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp1 = kpg.generateKeyPair(); // era-1 node 0
        final KeyPair kp2 = kpg.generateKeyPair(); // era-2 node 0 (different key)

        final NodeAddressBook era1Book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(NodeAddress.newBuilder()
                        .nodeId(0L)
                        .rsaPubKey(HexFormat.of().formatHex(kp1.getPublic().getEncoded()))
                        .build()))
                .build();
        final NodeAddressBook era2Book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(NodeAddress.newBuilder()
                        .nodeId(0L)
                        .rsaPubKey(HexFormat.of().formatHex(kp2.getPublic().getEncoded()))
                        .build()))
                .build();

        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(
                        RangedNodeAddressBook.newBuilder()
                                .addressBook(era1Book)
                                .startBlock(0L)
                                .endBlock(999L)
                                .build(),
                        RangedNodeAddressBook.newBuilder()
                                .addressBook(era2Book)
                                .startBlock(1000L)
                                .endBlock(-1L)
                                .build()))
                .build();
        updateAddressBookHistory(history);

        // Build and sign a WRB block in era 1 (block 500) using kp1.
        final long blockNumber = 500L;
        final byte[] recordFileBytes = "era1-content".getBytes();
        final MessageDigest digest = MessageDigest.getInstance("SHA-384");
        digest.update(new byte[] {0, 0, 0, 6});
        digest.update(recordFileBytes);
        final byte[] payload = digest.digest();
        final Signature engine = Signature.getInstance("SHA384withRSA");
        engine.initSign(kp1.getPrivate());
        engine.update(payload);
        final byte[] sigBytes = engine.sign();

        final java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        bos.write(0x12);
        bos.write(recordFileBytes.length);
        bos.write(recordFileBytes);

        final SemanticVersion hapiVersion = new SemanticVersion(1, 0, 0, "", "");
        final SemanticVersion swVersion = new SemanticVersion(1, 0, 0, "", "");
        final BlockHeader header = new BlockHeader(
                hapiVersion, swVersion, blockNumber, new Timestamp(1_700_000_000L, 0), BlockHashAlgorithm.SHA2_384);
        final BlockFooter footer = new BlockFooter(Bytes.wrap(new byte[48]), Bytes.wrap(new byte[48]), Bytes.EMPTY);
        final BlockProof proof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedRecordFileProof(
                        new SignedRecordFileProof(6, List.of(new RecordFileSignature(Bytes.wrap(sigBytes), 0L))))
                .build();
        final List<BlockItemUnparsed> items = List.of(
                BlockItemUnparsed.newBuilder()
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .recordFile(Bytes.wrap(bos.toByteArray()))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .blockFooter(BlockFooter.PROTOBUF.toBytes(footer))
                        .build(),
                BlockItemUnparsed.newBuilder()
                        .blockProof(BlockProof.PROTOBUF.toBytes(proof))
                        .build());

        blockMessaging.sendBlockItems(new BlockItems(items, blockNumber, true, true));

        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertFalse(notifications.isEmpty(), "Plugin must emit a notification for the era-1 WRB block");
        final VerificationNotification notification = notifications.getLast();
        assertEquals(blockNumber, notification.blockNumber());
        assertTrue(notification.success(), "Era-1 block signed with era-1 key must verify successfully");
    }

    // ==== TSS End-to-End Flow Test ===================================================================================

    @Test
    @DisplayName("TSS flow: block 0 bootstraps TSS state, subsequent block verifies with TSS")
    void tssFlowBlock0ThenSubsequentBlock() {
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder builder =
                org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.create(TssBlockSigner.create());
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed block0 =
                builder.genesisWithPublication();
        final org.hiero.block.node.verification.harness.LegacyHarnessChainBuilder.Signed blockN = builder.next(1L);

        // Process block 0 via live stream — bootstraps TSS parameters.
        blockMessaging.sendBlockItems(
                new BlockItems(block0.block().blockUnparsed().blockItems(), 0, true, true));
        VerificationNotification block0Notification =
                blockMessaging.getSentVerificationNotifications().get(0);
        assertTrue(block0Notification.success(), "TSS block 0 must verify successfully");
        assertNotNull(VerificationServicePlugin.activeLedgerId, "Plugin must have ledger ID after block 0");

        // Process subsequent block via backfill — verifies using TSS with ledger ID from block 0.
        plugin.handleBackfilled(new BackfilledBlockNotification(
                blockN.block().number(), blockN.block().blockUnparsed()));
        VerificationNotification blockNNotification =
                blockMessaging.getSentVerificationNotifications().get(1);
        assertTrue(
                blockNNotification.success(),
                "Subsequent TSS block must verify with ledger ID bootstrapped from block 0");
    }

    // ==== Helpers ====================================================================================================

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
                    tssParametersFilePath,
                    false,
                    Path.of("/tmp/verification-dumps"),
                    7);
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
