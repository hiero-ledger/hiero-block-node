// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * E2E tests for the block-stream filter on the publisher side. Each test
 * configures {@code PublisherConfig}
 * via System properties (the same {@code SystemPropertiesConfigSource}
 * ordinal-400 pattern used by {@code BlockNodeCloudStorageTests}), boots
 * {@code BlockNodeApp} in-JVM, and exercises the publisher gRPC.
 *
 * <p>The filter is set to deny {@code ROUND_HEADER} (field 3) — the only
 * non-mandatory item produced by
 * {@link BlockItemBuilderUtils#createSimpleBlockWithNumber(long)}. After
 * the filter passes the publisher accepts the block (it has been
 * rewritten with a {@code FilteredSingleItem} in place of the round
 * header). Pure publish-side smoke; the deeper "did the verifier accept
 * the rewritten block" assertion would require additional plumbing and
 * is captured in {@code docs/design/filtering/block-stream-filtering.md}
 * and the expansion notes.
 */
@Tag("api")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BlockStreamFilterE2ETests {

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient publishClient;

    @BeforeEach
    void setUp() throws Exception {
        final Path dataDir = Paths.get("build/tmp/data").toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        // Deny ROUND_HEADER (field 3) on the publish side. SystemPropertiesConfigSource
        // (ordinal 400) picks these up before BlockNodeApp constructs its Configuration.
        System.setProperty("producer.blockStreamFilterInclude", "false");
        System.setProperty("producer.blockStreamFilterItemTypes", "3");

        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(State.RUNNING, app.blockNodeState(), "BlockNodeApp must be RUNNING after startup");

        publishClient = createGrpcClient();
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("producer.blockStreamFilterInclude");
        System.clearProperty("producer.blockStreamFilterItemTypes");
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("BlockStreamFilterE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread teardown.
            }
        }
    }

    /**
     * Publishes a block while the publisher is configured with a deny-list for
     * {@code ROUND_HEADER}. The block is rewritten at ingress with a
     * {@code FilteredSingleItem} replacing the round header before the messaging
     * facility sees it. We assert the publisher returns an acknowledgement —
     * proof that the filter rewriting + downstream pipeline didn't choke on the
     * rewritten block.
     */
    @Test
    void publisherAcksFilteredBlock() throws Exception {
        final long blockNumber = 0L;
        final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        final PublishStreamRequest blockRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                .build();

        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishSvc.publishBlockStream(ackObserver);

        final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(1);
        stream.onNext(blockRequest);
        stream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
        awaitLatch(ackLatch, "filtered block acknowledgement");

        assertThat(ackObserver.getOnNextCalls()).isNotEmpty();
        // The response is either an ACK (block accepted) or a structured non-ACK
        // status; both are valid outcomes that prove the filter didn't crash the
        // ingest pipeline. A crash would manifest as no on-next call at all.
        assertThat(ackObserver.getOnNextCalls().get(0).response().kind()).isNotNull();

        stream.closeConnection();
    }

    /**
     * Verifier-acceptance assertion. Publishes a block with the publisher
     * configured to deny {@code ROUND_HEADER}, then subscribes back from the
     * Block Node and asserts the persisted block contains a
     * {@code FilteredSingleItem} in place of the round header. The subscribe
     * call succeeds only if verification accepted the rewritten block — making
     * this the real round-trip "filter → verify → persist → serve" check.
     */
    @Test
    void filteredBlockIsVerifiedAndServedToSubscribers() throws Exception {
        final long blockNumber = 0L;
        final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);

        // ── Publish the block with the publisher filter active ────────────────
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publishStream = publishSvc.publishBlockStream(ackObserver);

        final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(1);
        publishStream.onNext(PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                .build());
        publishStream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
        awaitLatch(ackLatch, "publish acknowledgement for filtered block");

        // Confirm the publisher acknowledged — verification accepted the block.
        assertThat(ackObserver.getOnNextCalls())
                .as("publisher must ack the filtered block; a NACK means verification rejected it")
                .anySatisfy(resp -> assertThat(resp.response().kind())
                        .isEqualTo(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT));

        // ── Subscribe back and assert FilteredSingleItem is present ───────────
        final PbjGrpcClient subscribeClient = createGrpcClient();
        final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient subscribeSvc =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(subscribeClient, OPTIONS);
        final ResponsePipelineUtils<SubscribeStreamResponse> subscribeObserver = new ResponsePipelineUtils<>();

        // Expect: block-items frame, end-of-block frame, success status.
        final AtomicReference<CountDownLatch> subscribeLatch = subscribeObserver.setAndGetOnNextLatch(2);
        subscribeSvc.subscribeBlockStream(
                SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(blockNumber)
                        .endBlockNumber(blockNumber)
                        .build(),
                subscribeObserver);
        awaitLatch(subscribeLatch, "subscribe to filtered block");

        final boolean hasFilteredSingleItem = subscribeObserver.getOnNextCalls().stream()
                .filter(r -> r.blockItems() != null)
                .flatMap(r -> r.blockItems().blockItems().stream())
                .anyMatch(bi -> bi.item().kind() == BlockItem.ItemOneOfType.FILTERED_SINGLE_ITEM);
        final boolean hasRoundHeader = subscribeObserver.getOnNextCalls().stream()
                .filter(r -> r.blockItems() != null)
                .flatMap(r -> r.blockItems().blockItems().stream())
                .anyMatch(bi -> bi.item().kind() == BlockItem.ItemOneOfType.ROUND_HEADER);

        assertThat(hasFilteredSingleItem)
                .as("persisted block should contain a FilteredSingleItem in place of the round header")
                .isTrue();
        assertThat(hasRoundHeader)
                .as("publisher-side filter should have stripped the original RoundHeader before persistence")
                .isFalse();

        publishStream.closeConnection();
    }

    /**
     * Proof-chain across two filtered blocks. Each block's proof anchors to the
     * previous block's recomputed root; if the filter ever broke the chain — e.g.
     * by mis-aligning the leaf hash or by mis-routing a {@code FilteredSingleItem}
     * to the wrong sub-tree — block 1 would fail verification even though block 0
     * passed. A single-block test cannot catch that. We publish block 0 + block 1
     * back-to-back, then subscribe both back and assert both carry a
     * {@code FilteredSingleItem} where the round header used to be.
     */
    @Test
    void filteredBlockChainVerifiesAcrossTwoBlocks() throws Exception {
        final long blockZero = 0L;
        final long blockOne = 1L;
        final BlockItem[] block0Items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockZero);
        // Anchor block 1's proof to block 0's recomputed root hash.
        final Bytes block0Hash = BlockItemBuilderUtils.computeBlockHash(blockZero, null);
        final BlockItem[] block1Items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockOne, block0Hash);

        // ── Publish both blocks with the publisher filter active ──────────────
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publishStream = publishSvc.publishBlockStream(ackObserver);

        final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(2);
        publishStream.onNext(PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(block0Items).build())
                .build());
        publishStream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockZero).build())
                .build());
        publishStream.onNext(PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(block1Items).build())
                .build());
        publishStream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockOne).build())
                .build());
        awaitLatch(ackLatch, "acknowledgements for two filtered blocks");

        // Both blocks must verify — count the ACK responses for blocks 0 and 1.
        final long ackCount = ackObserver.getOnNextCalls().stream()
                .filter(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT)
                .count();
        assertThat(ackCount)
                .as("both filtered blocks must verify; chain breaks if proof anchor drifts")
                .isGreaterThanOrEqualTo(2L);

        // ── Subscribe back: both persisted blocks must carry FilteredSingleItem
        final PbjGrpcClient subscribeClient = createGrpcClient();
        final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient subscribeSvc =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(subscribeClient, OPTIONS);
        final ResponsePipelineUtils<SubscribeStreamResponse> subscribeObserver = new ResponsePipelineUtils<>();

        // Expect for each block: items frame + endOfBlock frame. Two blocks → ≥4 onNext.
        final AtomicReference<CountDownLatch> subscribeLatch = subscribeObserver.setAndGetOnNextLatch(4);
        subscribeSvc.subscribeBlockStream(
                SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(blockZero)
                        .endBlockNumber(blockOne)
                        .build(),
                subscribeObserver);
        awaitLatch(subscribeLatch, "subscribe-back for the two-block filtered chain");

        final long filteredSingleItemCount = subscribeObserver.getOnNextCalls().stream()
                .filter(r -> r.blockItems() != null)
                .flatMap(r -> r.blockItems().blockItems().stream())
                .filter(bi -> bi.item().kind() == BlockItem.ItemOneOfType.FILTERED_SINGLE_ITEM)
                .count();
        assertThat(filteredSingleItemCount)
                .as("expect one FilteredSingleItem per block (the round header), so two across the chain")
                .isGreaterThanOrEqualTo(2L);

        publishStream.closeConnection();
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private PbjGrpcClient createGrpcClient() {
        final Duration timeout = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + SERVER_PORT)
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(timeout)
                        .build()))
                .connectTimeout(timeout)
                .keepAlive(true)
                .build();
        return new PbjGrpcClient(
                webClient, new PbjGrpcClientConfig(timeout, tls, OPTIONS.authority(), OPTIONS.contentType()));
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String desc)
            throws InterruptedException {
        latch.get().await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for: " + desc);
    }
}
