// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestVerificationPlugin;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Plugin-level regression tests for the previewnet 0.31.0-rc1 forwarder
/// deadlock. These tests exercise the full chain through the
/// [StreamPublisherPlugin]: gRPC bytes in -> forwarder -> messaging ->
/// persist -> ACK response out.
@DisplayName("StreamPublisherPlugin Regression Tests")
class StreamPublisherPluginRegressionTest
        extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService, ScheduledBlockingExecutor> {

    private static final Function<Bytes, PublishStreamResponse> RESPONSE_PARSER = bytes -> {
        try {
            return PublishStreamResponse.PROTOBUF.parse(bytes);
        } catch (final ParseException e) {
            throw new UncheckedParseException(e);
        }
    };
    private static final Function<PublishStreamResponse, ResponseOneOfType> RESPONSE_KIND =
            response -> response.response().kind();
    private static final Function<PublishStreamResponse, Long> ACK_BLOCK_NUMBER =
            response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();

    private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

    StreamPublisherPluginRegressionTest() {
        super(Executors.newSingleThreadExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        final StreamPublisherPlugin plugin = new StreamPublisherPlugin();
        final TestVerificationPlugin verificationPlugin = new TestVerificationPlugin();
        final List<BlockNodePlugin> additionalPlugins = List.of(verificationPlugin);
        start(plugin, plugin.methods().getFirst(), historicalBlockFacility, additionalPlugins);
    }

    /// Verifies that backfill persistence unblocks the forwarder when a
    /// publisher stalls mid-block.
    ///
    /// Publisher 1 streams blocks 0-4 (ACKed), then sends only the header
    /// for block 5 and goes silent. Publisher 2 receives SKIP for block 5.
    /// Backfill persists block 5; {@code handlePersisted(5)} triggers
    /// {@code checkForStalledHandlers(5)} which detects that handler 0
    /// holds ACCEPT for the same block that was just persisted externally
    /// (the BLOCK_ABANDONED path). This removes block 5's stale queue from
    /// {@code queueByBlockMap} and ends handler 0, unblocking the forwarder.
    /// Publisher 2 then streams blocks 6-9 and receives ACKs normally.
    @Test
    @DisplayName("backfill persistence resolves stalled block via BLOCK_ABANDONED detection")
    void testForwarderAdvancesPastStalledBlockAfterBackfill() {
        final long stalledBlock = 5L;

        // Publisher 1 streams blocks 0-4 and receives ACK for each.
        for (long blockNumber = 0; blockNumber <= 4L; blockNumber++) {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
            sendBlock(toPluginPipe, block);
            endThisBlock(toPluginPipe, blockNumber);
            awaitPluginResponses(1);
            assertThat(fromPluginBytes)
                    .as("publisher 1 must receive ACK for block %d", blockNumber)
                    .hasSize(1)
                    .first()
                    .extracting(RESPONSE_PARSER)
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, RESPONSE_KIND)
                    .returns(blockNumber, ACK_BLOCK_NUMBER);
            fromPluginBytes.clear();
        }

        // Disable the historical facility while block 5 stalls. The forwarder
        // sends block 5's partial header to messaging; disabling the facility
        // prevents that orphaned partial block from crashing the facility when
        // the forwarder later switches to block 6.
        historicalBlockFacility.setDisablePlugin();

        // Publisher 1 sends only the header for block 5, then goes silent.
        sendHeaderOnly(toPluginPipe, stalledBlock);

        // Publisher 2 connects and sends block 5 — expects SKIP.
        final TestPipeline publisher2 = createNewPipeline();
        final TestBlock fullBlock5 = TestBlockBuilder.generateBlockWithNumber(stalledBlock);
        sendBlock(publisher2.toPluginPipe(), fullBlock5);
        endThisBlock(publisher2.toPluginPipe(), stalledBlock);
        awaitPluginResponses(List.of(publisher2.fromPluginBytes()), 1);
        assertThat(publisher2.fromPluginBytes())
                .as("publisher 2 must receive SKIP for block %d", stalledBlock)
                .hasSize(1)
                .first()
                .extracting(RESPONSE_PARSER)
                .returns(ResponseOneOfType.SKIP_BLOCK, RESPONSE_KIND);
        publisher2.fromPluginBytes().clear();

        // Backfill persists block 5. handlePersisted(5) triggers
        // checkForStalledHandlers(5) which detects that handler 0 holds
        // ACCEPT for the same block (BLOCK_ABANDONED path). This removes
        // block 5's stale queue and ends handler 0, unblocking the forwarder.
        blockMessaging.sendBlockPersisted(new PersistedNotification(stalledBlock, true, 0, BlockSource.BACKFILL));

        // Re-enable the facility. The BLOCK_ABANDONED path has already
        // removed block 5's stale queue, so the forwarder will advance to
        // block 6 when it arrives.
        historicalBlockFacility.clearDisablePlugin();

        // Publisher 2 sends blocks 6-9. With the stall resolved by the
        // BLOCK_ABANDONED path, these blocks flow through the full chain
        // (forwarder -> messaging -> persist -> ACK) without needing a RESEND.
        for (long blockNumber = stalledBlock + 1; blockNumber <= stalledBlock + 4; blockNumber++) {
            sendBlock(publisher2.toPluginPipe(), TestBlockBuilder.generateBlockWithNumber(blockNumber));
            endThisBlock(publisher2.toPluginPipe(), blockNumber);
        }
        awaitPluginResponses(List.of(publisher2.fromPluginBytes()), 1);
        final List<PublishStreamResponse> ackResponses =
                publisher2.fromPluginBytes().stream().map(RESPONSE_PARSER).toList();
        assertThat(ackResponses)
                .as("publisher 2 must receive ACK after backfill resolved the stall")
                .anySatisfy(response -> assertThat(response).returns(ResponseOneOfType.ACKNOWLEDGEMENT, RESPONSE_KIND));

        // Block 6 must be persisted — it was forwarded through the full chain
        // after the BLOCK_ABANDONED path unblocked the forwarder.
        assertThat(historicalBlockFacility.block(stalledBlock + 1))
                .as("block %d must be stored after forwarder advanced past stall", stalledBlock + 1)
                .isNotNull();
    }

    private static void sendBlock(
            final com.hedera.pbj.runtime.grpc.Pipeline<? super Bytes> requestSender, final TestBlock block) {
        final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                .blockItems(block.asItemSetUnparsed())
                .build();
        requestSender.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
    }

    private static void sendHeaderOnly(
            final com.hedera.pbj.runtime.grpc.Pipeline<? super Bytes> requestSender, final long blockNumber) {
        final BlockItemSetUnparsed headerOnly = BlockItemSetUnparsed.newBuilder()
                .blockItems(List.of(
                        TestBlockBuilder.generateBlockWithNumber(blockNumber).getHeaderUnparsed()))
                .build();
        final PublishStreamRequestUnparsed request =
                PublishStreamRequestUnparsed.newBuilder().blockItems(headerOnly).build();
        requestSender.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
    }
}
