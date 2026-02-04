// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

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
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit test for {@link VerificationServicePlugin}.
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
        Path tempVerificationPath = tempDir.resolve("verificationData.bin");
        defaultConfig = VerificationConfigBuilder.newBuilder()
                .allBlocksHasherFilePath(tempVerificationPath)
                .allBlocksHasherEnabled(true)
                .allBlocksHasherPersistenceInterval(2)
                .toMap();
        start(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility(), defaultConfig);
    }

    @Test
    void testVerificationPlugin() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_68_0_BLOCK_14);

        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber));

        // check we received a block verification
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

    // @todo(2002): Re-enable once proper v0.71.0 verification is implemented with updated protobuf.
    // DummyVerificationSession always returns success, so failed verification cannot be tested.
    @Disabled("Disabled until proper v0.71.0 verification is implemented - see @todo(2002)")
    @Test
    void testFailedVerification() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_69_0_BLOCK_240);
        // get the original block items
        List<BlockItemUnparsed> originalItems = sampleBlockInfo.blockUnparsed().blockItems();
        // make a mutable copy
        List<BlockItemUnparsed> blockItems = new ArrayList<>(originalItems);

        // remove one block item, so the hash is no longer valid
        blockItems.remove(3);
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber));

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
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_68_0_BLOCK_14);
        long blockNumber = sampleBlockInfo.blockNumber();
        List<BlockItemUnparsed> originalItems = sampleBlockInfo.blockUnparsed().blockItems();

        // make a mutable copy
        List<BlockItemUnparsed> blockItems = new ArrayList<>(originalItems);

        // remove the header to simulate a case where receive items and have never received a header
        blockItems.removeFirst();
        // send some items to the plugin, they should be ignored
        plugin.handleBlockItemsReceived(new BlockItems(blockItems, blockNumber));
        // check we did not receive a block verification
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived with non-running server")
    void testHandleBlockItemsReceived_NotRunning() {
        // make the server state not running
        ((TestHealthFacility) blockNodeContext.serverHealth()).isRunning.set(false);
        // send some items to the plugin, they should be ignored
        plugin.handleBlockItemsReceived(
                new BlockItems(List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null))), -1));
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
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_68_0_BLOCK_14);

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
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_68_0_BLOCK_14);

        BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(
                sampleBlockInfo.blockUnparsed().blockItems().getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number() + 1;
        plugin.handleBlockItemsReceived(
                new BlockItems(sampleBlockInfo.blockUnparsed().blockItems(), blockNumber));

        // check we don't received a block verification notification
        long blockNotifications =
                blockMessaging.getSentVerificationNotifications().size();
        assertEquals(1, blockNotifications);
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
    }

    private static class VerificationConfigBuilder {

        // Fields with default values
        private Path allBlocksHasherFilePath;
        private boolean allBlocksHasherEnabled = true;
        private int allBlocksHasherPersistenceInterval = 10;

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

        public VerificationConfig build() {
            return new VerificationConfig(
                    allBlocksHasherFilePath, allBlocksHasherEnabled, allBlocksHasherPersistenceInterval);
        }

        public Map<String, String> toMap() {
            Map<String, String> configMap = new HashMap<>();
            configMap.put("verification.allBlocksHasherFilePath", allBlocksHasherFilePath.toString());
            configMap.put("verification.allBlocksHasherEnabled", String.valueOf(allBlocksHasherEnabled));
            configMap.put(
                    "verification.allBlocksHasherPersistenceInterval",
                    String.valueOf(allBlocksHasherPersistenceInterval));
            return configMap;
        }
    }
}
