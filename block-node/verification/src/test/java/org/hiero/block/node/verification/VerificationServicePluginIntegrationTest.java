// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils.SAMPLE_BLOCKS_WRB;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils.SampleBlockInfo;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Integration-style tests for {@link VerificationServicePlugin} using real WRB block fixtures.
 *
 * <p>The pipeline under test:
 * <ol>
 *   <li>Load a real WRB (Wrapped Record Block) from {@code BlockUtils.SAMPLE_BLOCKS_WRB}.</li>
 *   <li>Load the matching RSA {@link NodeAddressBook} via
 *       {@link BlockUtils#getSampleAddressBook(String)}.</li>
 *   <li>Deliver the address book to the plugin via
 *       {@link PluginTestBase#updateAddressBook(NodeAddressBook)}, which simulates the production
 *       {@code ApplicationStateFacility.updateAddressBook()} → context rebuild →
 *       {@code onContextUpdate()} chain without requiring a running scheduler.</li>
 *   <li>Send the block through the plugin's live-stream handler via
 *       {@link org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility}.</li>
 *   <li>Assert the resulting {@link VerificationNotification}.</li>
 * </ol>
 *
 * <h2>Adding new test fixtures</h2>
 *
 * <p>Place files under {@code block-node/app/src/testFixtures/resources/}:
 *
 * <pre>
 * test-blocks/
 *   WRB/
 *     &lt;network&gt;/
 *       &lt;block-number&gt;.blk.gz     — GZIP-compressed protobuf BlockUnparsed
 *       address-book.json          — NodeAddressBook (SPKI in rsaPubKey), PBJ JSON codec
 * </pre>
 *
 * <p>The {@code .blk.gz} format is identical to the TSS block fixtures under
 * {@code test-blocks/CN_0_73_TSS_WRAPS/}: GZIP wrapping a protobuf-encoded {@link BlockUnparsed}.
 *
 * <p>The {@code address-book.json} file is the PBJ JSON serialisation of {@link NodeAddressBook}
 * whose {@code rsaPubKey} entries are hex-encoded X.509 {@code SubjectPublicKeyInfo} bytes (the
 * only shape {@link RsaKeyDecoder} currently accepts). See {@code WrbAddressBookFixtureGeneratorTest}
 * for how this file is produced from a {@code genesis-network.json}.
 *
 * <p>After adding a new fixture, append it to {@link SAMPLE_BLOCKS_WRB} so it is automatically
 * picked up by {@link #realWrbBlock_verifiesSuccessfullyAgainstMatchingAddressBook}.
 */
class VerificationServicePluginIntegrationTest
        extends PluginTestBase<VerificationServicePlugin, BlockingExecutor, ScheduledExecutorService> {

    VerificationServicePluginIntegrationTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        Objects.requireNonNull(tempDir);
        // Reset static TSS state for test isolation.
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;

        final Map<String, String> config = new HashMap<>();
        config.put(
                "verification.allBlocksHasherFilePath",
                tempDir.resolve("hasher.bin").toString());
        config.put("verification.allBlocksHasherEnabled", "true");
        config.put("verification.allBlocksHasherPersistenceInterval", "10");
        config.put(
                "verification.tssParametersFilePath",
                tempDir.resolve("tss-params.bin").toString());

        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);
    }

    // -------------------------------------------------------------------------
    // Happy path — every WRB sample verifies against its matching address book
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @EnumSource(SAMPLE_BLOCKS_WRB.class)
    @DisplayName("Real WRB block verifies successfully against its matching address book")
    void realWrbBlock_verifiesSuccessfullyAgainstMatchingAddressBook(final SAMPLE_BLOCKS_WRB sample)
            throws IOException, ParseException {
        final NodeAddressBook book = BlockUtils.getSampleAddressBook(sample.network());
        final SampleBlockInfo blockInfo = BlockUtils.getSampleBlockInfo(sample);

        final VerificationNotification notification = runWrbVerification(book, blockInfo);

        assertNotNull(notification, "Plugin must emit a VerificationNotification");
        assertTrue(
                notification.success(),
                "Real WRB block " + sample + " must verify successfully against its " + sample.network()
                        + " address book");
        assertNotNull(notification.blockHash(), "Block hash must be set on successful verification");
        assertNotNull(notification.block(), "Block content must be present on successful verification");
        assertEquals(
                sample.getBlockNumber(),
                notification.blockNumber(),
                "Notification must echo the block number from the header");
    }

    // -------------------------------------------------------------------------
    // Negative path — without a loaded address book the verifier must reject
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("WRB block is rejected when no RSA address book has been published yet")
    void realWrbBlock_rejectedWithoutAddressBook() throws IOException, ParseException {
        final SampleBlockInfo soloBlock = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_WRB.SOLO_4N_BLOCK_0);

        final VerificationNotification notification = runWrbVerification(NodeAddressBook.DEFAULT, soloBlock);

        assertNotNull(notification, "Plugin must emit a VerificationNotification");
        assertFalse(
                notification.success(),
                "Solo-network block must NOT verify when the RSA key map is empty (RsaRosterBootstrapPlugin not yet ready)");
        assertNull(notification.blockHash(), "Block hash must be null on a rejected verification");
        assertNull(notification.block(), "Block content must be null on a rejected verification");
    }

    @Test
    @DisplayName("WRB block is rejected with BAD_BLOCK_PROOF when signed by keys that do not match the address book")
    void realWrbBlock_rejectedWithWrongKeys() throws Exception {
        // Build an address book that claims the same node IDs the solo capture uses (0..3) but
        // whose RSA public keys are freshly generated — i.e. signatures over the real payload will
        // never verify under these keys. This exercises the engine.verify(...) == false branch in
        // ExtendedMerkleTreeSession#verifyRsaProof, which returns BAD_BLOCK_PROOF immediately on
        // the first signature that fails to verify.
        final NodeAddressBook wrongKeysBook = buildAddressBookWithFreshRsaKeys(4);
        final SampleBlockInfo soloBlock = BlockUtils.getSampleBlockInfo(SAMPLE_BLOCKS_WRB.SOLO_4N_BLOCK_0);

        final VerificationNotification notification = runWrbVerification(wrongKeysBook, soloBlock);

        assertNotNull(notification, "Plugin must emit a VerificationNotification");
        assertFalse(notification.success(), "Block must NOT verify against an address book containing wrong RSA keys");
        assertEquals(
                VerificationNotification.FailureType.BAD_BLOCK_PROOF,
                notification.failureType(),
                "Failed cryptographic verification must surface as BAD_BLOCK_PROOF");
        assertNull(notification.blockHash(), "Block hash must be null on a rejected verification");
        assertNull(notification.block(), "Block content must be null on a rejected verification");
    }

    private static NodeAddressBook buildAddressBookWithFreshRsaKeys(final int nodeCount) throws Exception {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final HexFormat hex = HexFormat.of();
        final List<NodeAddress> entries = new ArrayList<>(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            final KeyPair kp = kpg.generateKeyPair();
            entries.add(NodeAddress.newBuilder()
                    .nodeId(nodeId)
                    .rsaPubKey(hex.formatHex(kp.getPublic().getEncoded()))
                    .build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(entries).build();
    }

    // -------------------------------------------------------------------------
    // Harness
    // -------------------------------------------------------------------------

    /**
     * Runs the full WRB verification pipeline:
     * <ol>
     *   <li>Delivers {@code book} to the plugin via
     *       {@link PluginTestBase#updateAddressBook(NodeAddressBook)} — this simulates
     *       {@code ApplicationStateFacility.updateAddressBook()} and synchronously triggers
     *       {@code plugin.onContextUpdate()}, rebuilding the RSA key map.</li>
     *   <li>Sends every block item through the plugin's live-stream handler in a single
     *       {@link BlockItems} batch (start-of-block = true, end-of-block = true).</li>
     *   <li>Returns the last {@link VerificationNotification} emitted by the plugin.</li>
     * </ol>
     */
    private VerificationNotification runWrbVerification(final NodeAddressBook book, final SampleBlockInfo blockInfo) {
        updateAddressBook(book);

        final BlockUnparsed block = blockInfo.blockUnparsed();
        final long blockNumber = blockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(block.blockItems(), blockNumber, true, true));

        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertFalse(notifications.isEmpty(), "Plugin must emit at least one VerificationNotification");
        return notifications.getLast();
    }
}
