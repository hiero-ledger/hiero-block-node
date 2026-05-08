// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration-style tests for {@link VerificationServicePlugin} using real WRB block fixtures.
 *
 * <p>These tests exercise the full plugin pipeline:
 * <ol>
 *   <li>Load a real WRB (Wrapped Record Block) from a test resource file.</li>
 *   <li>Load the matching RSA {@link NodeAddressBook} (the public keys of the nodes that signed
 *       the block).</li>
 *   <li>Deliver the address book to the plugin via
 *       {@link PluginTestBase#updateAddressBook(NodeAddressBook)}, which simulates the production
 *       {@code ApplicationStateFacility.updateAddressBook()} → context rebuild → *       {@code onContextUpdate()} chain without requiring a running scheduler.</li>
 *   <li>Send the block through the plugin's live-stream handler via
 *       {@link org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility}.</li>
 *   <li>Assert the resulting {@link VerificationNotification}.</li>
 * </ol>
 *
 * <h2>Adding new test fixtures</h2>
 *
 * <p>Place files under {@code block-node/verification/src/test/resources/}:
 *
 * <pre>
 * test-blocks/
 *   wrb/
 *     &lt;network&gt;/
 *       &lt;block-number&gt;.blk.gz     — GZIP-compressed protobuf BlockUnparsed
 * test-data/
 *   wrb/
 *     &lt;network&gt;/
 *       address-book.pb            — protobuf-serialised NodeAddressBook (RSA public keys)
 * </pre>
 *
 * <p>The {@code .blk.gz} format is identical to the TSS block fixtures under
 * {@code test-blocks/CN_0_73_TSS_WRAPS/}: GZIP wrapping a protobuf-encoded {@link BlockUnparsed}.
 *
 * <p>The {@code address-book.pb} file is a raw protobuf serialisation of {@link NodeAddressBook}
 * containing the RSA public keys ({@code rsaPubKey} field, hex-encoded DER) of the nodes that
 * produced the block.  Generate it with:
 * <pre>
 *   // Kotlin / Java snippet:
 *   val book = NodeAddressBook.newBuilder()
 *       .nodeAddress(/* ... node entries ... *{@literal /})
 *       .build()
 *   Files.write(Path.of("address-book.pb"), NodeAddressBook.PROTOBUF.toBytes(book).toByteArray())
 * </pre>
 *
 * <p>Alternatively, obtain the address book from the Mirror Node REST API
 * ({@code GET /api/v1/network/nodes}) and convert it using {@code RsaRosterBootstrapPlugin}'s
 * parsing logic, or export it from the block node's own persisted {@code rsa-bootstrap-roster.json}.
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
    // Placeholder test — enable once real WRB fixtures are available
    // -------------------------------------------------------------------------

    /**
     * Placeholder integration test for a real WRB block.
     *
     * <p>To activate:
     * <ol>
     *   <li>Remove {@code @Disabled}.</li>
     *   <li>Place the WRB block at
     *       {@code src/test/resources/test-blocks/wrb/<network>/<block-number>.blk.gz}.</li>
     *   <li>Place the matching address book at
     *       {@code src/test/resources/test-data/wrb/<network>/address-book.pb}.</li>
     *   <li>Update the resource paths in this test accordingly.</li>
     * </ol>
     */
    @Disabled("No WRB block fixtures available yet — see class Javadoc for setup instructions")
    @Test
    @DisplayName("real WRB block with matching address book verifies successfully")
    void realWrbBlock_withMatchingAddressBook_verifies() throws IOException, ParseException {
        final NodeAddressBook book = loadAddressBook("test-data/wrb/<network>/address-book.pb");
        final BlockUnparsed block = loadWrbBlock("test-blocks/wrb/<network>/<block-number>.blk.gz");

        final VerificationNotification notification = runWrbVerification(book, block);

        assertNotNull(notification, "Plugin must emit a VerificationNotification");
        assertTrue(notification.success(), "Real WRB block must verify successfully with the matching address book");
        assertNotNull(notification.blockHash(), "Block hash must be set on successful verification");
        assertNotNull(notification.block(), "Block content must be present on successful verification");
    }

    // -------------------------------------------------------------------------
    // Harness helpers
    // -------------------------------------------------------------------------

    /**
     * Loads a GZIP-compressed, protobuf-encoded {@link BlockUnparsed} from the given resource
     * path.  Resources are resolved from this module's test classpath.
     *
     * @param resourcePath path relative to the test resource root,
     *     e.g. {@code "test-blocks/wrb/mainnet/1234567.blk.gz"}
     * @return the parsed {@link BlockUnparsed}
     * @throws IOException if the resource cannot be opened or read
     * @throws ParseException if the protobuf payload cannot be parsed
     */
    private BlockUnparsed loadWrbBlock(final String resourcePath) throws IOException, ParseException {
        try (InputStream raw = getClass().getModule().getResourceAsStream(resourcePath)) {
            if (raw == null) {
                throw new IOException("WRB block resource not found: " + resourcePath
                        + " — see class Javadoc for fixture setup instructions");
            }
            try (GZIPInputStream gzip = new GZIPInputStream(raw)) {
                return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzip.readAllBytes()));
            }
        }
    }

    /**
     * Loads a protobuf-encoded {@link NodeAddressBook} from the given resource path.
     * The file must be a raw serialisation of {@link NodeAddressBook} (not GZIP-compressed).
     *
     * @param resourcePath path relative to the test resource root,
     *     e.g. {@code "test-data/wrb/mainnet/address-book.pb"}
     * @return the parsed {@link NodeAddressBook}
     * @throws IOException if the resource cannot be opened or read
     * @throws ParseException if the protobuf payload cannot be parsed
     */
    private NodeAddressBook loadAddressBook(final String resourcePath) throws IOException, ParseException {
        try (InputStream raw = getClass().getModule().getResourceAsStream(resourcePath)) {
            if (raw == null) {
                throw new IOException("Address book resource not found: " + resourcePath
                        + " — see class Javadoc for fixture setup instructions");
            }
            return NodeAddressBook.PROTOBUF.parse(Bytes.wrap(raw.readAllBytes()));
        }
    }

    /**
     * Runs the full WRB verification pipeline:
     * <ol>
     *   <li>Delivers {@code book} to the plugin via
     *       {@link PluginTestBase#updateAddressBook(NodeAddressBook)} — this simulates
     *       {@code ApplicationStateFacility.updateAddressBook()} and synchronously triggers
     *       {@code plugin.onContextUpdate()}, rebuilding the RSA key map.</li>
     *   <li>Parses the block number from the block's first item (the {@code BlockHeader}).</li>
     *   <li>Sends all block items through the plugin's live-stream handler in a single
     *       {@link BlockItems} batch (start-of-block = true, end-of-block = true).</li>
     *   <li>Returns the last {@link VerificationNotification} emitted by the plugin.</li>
     * </ol>
     *
     * @param book  the RSA address book matching the signing nodes for {@code block}
     * @param block the WRB block to verify
     * @return the {@link VerificationNotification} produced by the plugin
     * @throws ParseException if the block header cannot be parsed to extract the block number
     */
    private VerificationNotification runWrbVerification(final NodeAddressBook book, final BlockUnparsed block)
            throws ParseException {
        // Deliver the address book — triggers ApplicationStateFacility.updateAddressBook() flow.
        updateAddressBook(book);

        // Parse the block number from the header so BlockItems is constructed correctly.
        final long blockNumber = com.hedera.hapi.block.stream.output.BlockHeader.PROTOBUF
                .parse(block.blockItems().getFirst().blockHeaderOrThrow())
                .number();

        // Send the entire block as a single batch (WRB blocks are always self-contained).
        blockMessaging.sendBlockItems(new BlockItems(block.blockItems(), blockNumber, true, true));

        // Return the notification produced for this block.
        final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertFalse(notifications.isEmpty(), "Plugin must emit at least one VerificationNotification");
        return notifications.getLast();
    }
}
