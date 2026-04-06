// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for verification bugs found in production.
 */
class VerificationRegressionTest
        extends PluginTestBase<VerificationServicePlugin, BlockingExecutor, ScheduledExecutorService> {

    private final Map<String, String> defaultConfig;

    public VerificationRegressionTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
        defaultConfig = VerificationConfigBuilder.newBuilder()
                .allBlocksHasherFilePath(tempDir.resolve("verificationData.bin"))
                .allBlocksHasherEnabled(true)
                .tssParametersFilePath(tempDir.resolve("tss-parameters.bin"))
                .toMap();
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), defaultConfig);
    }

    /**
     * When a block is backfilled and then the same block arrives via the live-stream, the
     * live-stream verification fails with PUBLISHER source. This triggers BAD_BLOCK_PROOF,
     * disconnects all publishers, and the node enters a NODE_BEHIND loop it cannot escape.
     *
     * <p>Backfill appends the block hash to the allBlocksHasher, so getRootOfAllPreviousBlocks()
     * returns a root that already includes the block being verified — causing a hash mismatch.
     */
    @Test
    @DisplayName("backfilled block via live-stream should not emit PUBLISHER failure (hasher enabled)")
    void backfilledBlockWithHasherEnabledShouldNotCausePublisherFailure() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo block0 = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_0);
        BlockUtils.SampleBlockInfo block1 = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_1);

        // Block 0 via live stream — establishes verification state
        blockMessaging.sendBlockItems(
                new BlockItems(block0.blockUnparsed().blockItems(), block0.blockNumber(), true, true));

        // Block 1 via backfill — succeeds, but pollutes allBlocksHasher
        plugin.handleBackfilled(new BackfilledBlockNotification(block1.blockNumber(), block1.blockUnparsed()));

        // Same block 1 via live stream — reconnecting publisher resends it
        plugin.handleBlockItemsReceived(
                new BlockItems(block1.blockUnparsed().blockItems(), block1.blockNumber(), true, true));

        List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertTrue(notifications.get(0).success(), "Block 0 live-stream should succeed");
        assertTrue(notifications.get(1).success(), "Block 1 backfill should succeed");
        assertEquals(BlockSource.BACKFILL, notifications.get(1).source());

        // Block 1 via live-stream should be skipped (already verified via backfill),
        // so no PUBLISHER failure notification should be emitted
        for (VerificationNotification notification : notifications) {
            assertTrue(
                    notification.success() || notification.source() != BlockSource.PUBLISHER,
                    "Must not emit PUBLISHER failure for a block already verified via backfill. "
                            + "Actual: success=" + notification.success()
                            + ", source=" + notification.source());
        }
    }

    /**
     * Backfilling multiple blocks does not update {@code previousBlockHash}. When the next
     * expected block arrives via live stream, verification computes the wrong block hash
     * because it still uses the pre-backfill previous hash — causing a PUBLISHER failure.
     *
     * <p>Uses synthetic blocks with hash-of-hash signatures so the test exercises real
     * verification via {@code ExtendedMerkleTreeSession}.
     */
    @Test
    @DisplayName("stale previousBlockHash after multi-block backfill causes PUBLISHER failure")
    void stalePreviousBlockHashAfterMultiBlockBackfillShouldNotCausePublisherFailure() {
        VerificationServicePlugin.activeLedgerId = null;
        VerificationServicePlugin.activeTssPublication = null;
        VerificationServicePlugin.tssParametersPersisted = false;
        blockMessaging = new TestBlockMessagingFacility();
        Map<String, String> config = new HashMap<>(defaultConfig);
        config.put("verification.allBlocksHasherEnabled", "false");
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), config);

        Bytes hash0 = VerifiableBlockFactory.computeBlockHash(0, null);
        Bytes hash1 = VerifiableBlockFactory.computeBlockHash(1, hash0);
        Bytes hash2 = VerifiableBlockFactory.computeBlockHash(2, hash1);

        List<BlockItemUnparsed> block0Items = VerifiableBlockFactory.createBlockItems(0, null);
        List<BlockItemUnparsed> block1Items = VerifiableBlockFactory.createBlockItems(1, hash0);
        List<BlockItemUnparsed> block2Items = VerifiableBlockFactory.createBlockItems(2, hash1);
        List<BlockItemUnparsed> block3Items = VerifiableBlockFactory.createBlockItems(3, hash2);

        // Block 0 via live stream — establishes verification state
        blockMessaging.sendBlockItems(new BlockItems(block0Items, 0, true, true));

        // Backfill blocks 1 and 2 — succeed, but do NOT update previousBlockHash
        plugin.handleBackfilled(new BackfilledBlockNotification(1, new BlockUnparsed(block1Items)));
        plugin.handleBackfilled(new BackfilledBlockNotification(2, new BlockUnparsed(block2Items)));

        // Block 3 via live stream — should use hash(block2) as previousBlockHash,
        // but plugin still holds hash(block0)
        plugin.handleBlockItemsReceived(new BlockItems(block3Items, 3, true, true));

        List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications();
        assertTrue(notifications.get(0).success(), "Block 0 live-stream should succeed");
        assertTrue(notifications.get(1).success(), "Block 1 backfill should succeed");
        assertTrue(notifications.get(2).success(), "Block 2 backfill should succeed");

        VerificationNotification liveStreamBlock3 = notifications.get(3);
        assertTrue(
                liveStreamBlock3.success() || liveStreamBlock3.source() != BlockSource.PUBLISHER,
                "Must not emit PUBLISHER failure for block after backfill gap. "
                        + "Actual: success=" + liveStreamBlock3.success()
                        + ", source=" + liveStreamBlock3.source());
    }

    private static class VerificationConfigBuilder {

        private Path allBlocksHasherFilePath;
        private boolean allBlocksHasherEnabled = true;
        private Path tssParametersFilePath = Path.of("");

        static VerificationConfigBuilder newBuilder() {
            return new VerificationConfigBuilder();
        }

        VerificationConfigBuilder allBlocksHasherEnabled(boolean value) {
            this.allBlocksHasherEnabled = value;
            return this;
        }

        VerificationConfigBuilder allBlocksHasherFilePath(Path value) {
            this.allBlocksHasherFilePath = value;
            return this;
        }

        VerificationConfigBuilder tssParametersFilePath(Path value) {
            this.tssParametersFilePath = value;
            return this;
        }

        Map<String, String> toMap() {
            Map<String, String> configMap = new HashMap<>();
            configMap.put("verification.allBlocksHasherFilePath", allBlocksHasherFilePath.toString());
            configMap.put("verification.allBlocksHasherEnabled", String.valueOf(allBlocksHasherEnabled));
            configMap.put("verification.allBlocksHasherPersistenceInterval", "10");
            configMap.put("verification.tssParametersFilePath", tssParametersFilePath.toString());
            return configMap;
        }
    }
}
