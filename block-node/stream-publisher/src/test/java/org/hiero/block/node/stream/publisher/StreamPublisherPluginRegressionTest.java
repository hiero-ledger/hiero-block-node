// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.util.concurrent.locks.LockSupport.parkNanos;
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
    /// Backfill persists block 5 via [PersistedNotification]. Publisher 2
    /// then streams block 6. The full chain must deliver an ACK for block 6.
    ///
    /// The bug: {@code clearObsoleteQueueItems(5)} uses
    /// {@code headMap(5)} (strictly less than), so block 5's incomplete
    /// queue is never removed. The forwarder's
    /// {@code determineCurrentBlockNumber()} picks it via
    /// {@code firstEntry()} and gets stuck forever — block 6 never
    /// reaches messaging.
    @Test
    @DisplayName("forwarder delivers block N+1 after backfill persists stalled block N — previewnet 0.31.0-rc1")
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

        // Disable the historical facility while block 5 stalls. The
        // forwarder will send block 5's partial header to messaging, but
        // the facility ignores it — preventing an orphaned partial block
        // that would crash when block 6 arrives later.
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

        // Simulate LIVE_TAIL greedy backfill persisting block 5 from a peer.
        blockMessaging.sendBlockPersisted(new PersistedNotification(stalledBlock, true, 0, BlockSource.BACKFILL));

        // Re-enable the historical facility now that block 5's stalled
        // queue has been cleared by the headMap fix.
        historicalBlockFacility.clearDisablePlugin();

        // Publisher 2 streams block 6.
        final long nextLiveBlock = stalledBlock + 1;
        final TestBlock block6 = TestBlockBuilder.generateBlockWithNumber(nextLiveBlock);
        sendBlock(publisher2.toPluginPipe(), block6);
        endThisBlock(publisher2.toPluginPipe(), nextLiveBlock);

        // Publisher 2 streams block 7 — the manager ACCEPTs it (decision
        // layer works), but the forwarder is stuck on block 5's stalled
        // queue so neither block 6 nor 7 will ever be forwarded or ACKed.
        final long block7Number = nextLiveBlock + 1;
        final TestBlock block7 = TestBlockBuilder.generateBlockWithNumber(block7Number);
        sendBlock(publisher2.toPluginPipe(), block7);
        endThisBlock(publisher2.toPluginPipe(), block7Number);

        // Wait long enough for any async processing to complete.
        parkNanos(500_000_000L);

        // Publisher 2 must receive ACK for blocks 6 and 7 — the full chain:
        // forwarder -> messaging -> historical facility -> persist -> ACK.
        // The bug: forwarder is stuck on block 5's incomplete queue, so
        // blocks 6 and 7 are silently dropped — no ACK is ever sent.
        final List<PublishStreamResponse> publisher2Responses =
                publisher2.fromPluginBytes().stream().map(RESPONSE_PARSER).toList();
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block %d", nextLiveBlock)
                .anySatisfy(response -> assertThat(response)
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, RESPONSE_KIND)
                        .returns(nextLiveBlock, ACK_BLOCK_NUMBER));
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block %d", block7Number)
                .anySatisfy(response -> assertThat(response)
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, RESPONSE_KIND)
                        .returns(block7Number, ACK_BLOCK_NUMBER));
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
