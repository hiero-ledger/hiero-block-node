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

    /// Reproduces the previewnet 0.31.0-rc1 forwarder deadlock end-to-end
    /// through the plugin.
    ///
    /// Publisher 1 streams blocks 0-4 (ACKed), then sends only the header
    /// for block 5 and goes silent. Publisher 2 receives SKIP for block 5.
    /// Backfill persists block 5, but due to the original bug this alone
    /// does not unblock the forwarder. Publisher 2 then streams blocks 6-9;
    /// completing block 9 triggers stall detection ({@code 9 > 5+3}), which
    /// removes the stale queue and sends RESEND(5) to publisher 2. Publisher
    /// 2 re-sends block 5 and the full chain delivers an ACK.
    ///
    /// The bug: {@code correctForResendAndStreaming(5)} sees block 5 in
    /// {@code queueByBlockMap} and returns 4, so the stale queue is never
    /// removed. The forwarder's {@code determineCurrentBlockNumber()} picks
    /// it via {@code firstEntry()} and gets stuck forever. Stall detection
    /// ({@code completedBlockNumber > stalledBlock + maxBlocksBeforeStalled})
    /// eventually clears the stale queue and issues RESEND(5).
    @Test
    @DisplayName("stall detection recovers a block stalled after backfill — previewnet 0.31.0-rc1")
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

        // Backfill persists block 5. Without the headMap fix,
        // correctForResendAndStreaming(5) sees block 5 in queueByBlockMap and
        // returns 4 — the stale queue is never removed and the forwarder stays stuck.
        blockMessaging.sendBlockPersisted(new PersistedNotification(stalledBlock, true, 0, BlockSource.BACKFILL));

        // Publisher 2 sends blocks 6-9. The forwarder remains stuck on block 5's
        // stale queue so these blocks accumulate in queueByBlockMap without being
        // forwarded. endOfBlock(9) triggers checkForStalledHandlers: 9 > 5+3=8,
        // so block 5 is marked stalled, its queue is removed, and publisher 2
        // receives RESEND(5).
        for (long blockNumber = stalledBlock + 1; blockNumber <= stalledBlock + 4; blockNumber++) {
            sendBlock(publisher2.toPluginPipe(), TestBlockBuilder.generateBlockWithNumber(blockNumber));
            endThisBlock(publisher2.toPluginPipe(), blockNumber);
        }
        awaitPluginResponses(List.of(publisher2.fromPluginBytes()), 1);
        assertThat(publisher2.fromPluginBytes())
                .as("publisher 2 must receive RESEND for stalled block %d", stalledBlock)
                .hasSize(1)
                .first()
                .extracting(RESPONSE_PARSER)
                .returns(ResponseOneOfType.RESEND_BLOCK, RESPONSE_KIND);
        publisher2.fromPluginBytes().clear();

        // Re-enable the facility. The forwarder thread sleeps in waitForDataReady
        // (5ms timeout after stall detection fires), so the facility is reliably
        // re-enabled before any block is forwarded to it.
        historicalBlockFacility.clearDisablePlugin();

        // Publisher 2 re-sends block 5. The manager accepts it from blocksToResend.
        sendBlock(publisher2.toPluginPipe(), fullBlock5);
        endThisBlock(publisher2.toPluginPipe(), stalledBlock);

        // Wait for ACK(5) — the full chain: forwarder -> messaging -> persist -> ACK.
        awaitPluginResponses(List.of(publisher2.fromPluginBytes()), 1);
        final List<PublishStreamResponse> ackResponses =
                publisher2.fromPluginBytes().stream().map(RESPONSE_PARSER).toList();
        assertThat(ackResponses)
                .as("publisher 2 must receive ACK for block %d after stall detection recovery", stalledBlock)
                .anySatisfy(response -> assertThat(response)
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, RESPONSE_KIND)
                        .returns(stalledBlock, ACK_BLOCK_NUMBER));

        // Block 5 must be persisted — backfill alone could not recover it, but
        // stall detection + RESEND delivered it through the full persistence chain.
        assertThat(historicalBlockFacility.block(stalledBlock))
                .as("block %d must be stored after stall detection recovery", stalledBlock)
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
