// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * E2E regression tests for production bugs that required full-stack exercise to reproduce.
 * Each test documents the original failure mode in its javadoc and asserts the fixed behaviour.
 */
@Tag("api")
@DisplayName("BlockNodeApp Regression Tests")
public class BlockNodeApiRegressionTest {

    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

    private final Function<PublishStreamResponse, PublishStreamResponse.ResponseOneOfType> responseKindExtractor =
            response -> response.response().kind();
    private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
            response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();
    private final Function<PublishStreamResponse, Long> resendBlockNumberExtractor =
            response -> Objects.requireNonNull(response.resendBlock()).blockNumber();

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient publishBlockStreamPbjGrpcClient;
    private PbjGrpcClient getBlockPbjGrpcClient;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        final Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        try {
            assertNotNull(app, "BlockNodeApp should be constructed");
            app.start();
            Thread.sleep(200);
            assertEquals(State.RUNNING, app.blockNodeState());
            publishBlockStreamPbjGrpcClient = createGrpcClient();
            getBlockPbjGrpcClient = createGrpcClient();
        } catch (final Exception e) {
            if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
                app.shutdown("BlockNodeApiRegressionTest", "test-setup-failure");
            }
            throw e;
        }
    }

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BlockNodeApiRegressionTest", "test teardown");
        }
    }

    /**
     * Reproduces the missing-block gap caused by a publisher severing its connection mid-block.
     *
     * <p>Publisher 1 streams blocks 0–2 (fully ACKed), then sends only the header for block 3
     * and severs the connection. This adds block 3 to {@code blocksToResend} via
     * {@code blockIsEnding(3)}.
     *
     * <p>Publisher 2 then streams block 4. {@code endOfBlock(4)} returns {@code RESEND(3)}
     * because {@code blocksToResend} contains 3. Publisher 2 responds by providing the full
     * block 3, filling the gap. Block 5 follows normally.
     *
     * <p>All three blocks must end up in storage. Without the fix, block 3's queue is never
     * forwarded to messaging and the gap persists silently.
     */
    @Test
    @DisplayName("publisher RESEND fills block gap left by severed connection")
    void missingBlockGapWhenPublisherSeversConnectionBeforeEndOfBlock() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);

        // Publisher 1 — stream blocks 0–2 with proper hash chain, ACK for each.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher1Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher1Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher1Stream =
                publisher1Client.publishBlockStream(publisher1Observer);

        final Bytes[] previousHashes = {null, hash0, hash1};
        for (long blockNum = 0L; blockNum <= 2L; blockNum++) {
            final BlockItem[] items =
                    BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNum, previousHashes[(int) blockNum]);
            final AtomicReference<CountDownLatch> ackLatch = publisher1Observer.setAndGetOnNextLatch(1);
            publisher1Stream.onNext(buildPublishRequest(items));
            endBlock(blockNum, publisher1Stream);
            awaitLatch(ackLatch, "ACK for block " + blockNum);
        }

        // Publisher 1 sends only the header for block 3 (no proof), then severs via RESET.
        // blockIsEnding(3) removes block 3's queue from queueByBlockMap and adds 3 to
        // blocksToResend. No partial proof is stranded in blockProofs because no proof was sent.
        publisher1Stream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(3L)}));
        final AtomicReference<CountDownLatch> publisher1DisconnectLatch =
                publisher1Observer.setAndGetConnectionEndedLatch(1);
        publisher1Stream.onNext(PublishStreamRequest.newBuilder()
                .endStream(PublishStreamRequest.EndStream.newBuilder()
                        .endCode(PublishStreamRequest.EndStream.Code.RESET)
                        .build())
                .build());
        awaitLatch(publisher1DisconnectLatch, "publisher 1 disconnect after block 3 header");

        // Publisher 2 — connect and send block 4, then block 3 (the gap), then block 5.
        //
        // Sequence on the server:
        //   endOfBlock(4) -> RESEND(3): handler resets, block 4 stays in queueByBlockMap
        //   block 3 header -> getActionForHeader(3) -> ACCEPT (removes 3 from blocksToResend)
        //   endOfBlock(3) -> ACCEPT(3)
        //   endOfBlock(5) -> ACCEPT(5)
        //
        // Forwarder persistence order: block 4 first, then block 3, then block 5.
        // ACK(4) and ACK(5) are sent. Block 3 is persisted but implicit in ACK(4) (4 > 3).
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher2Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher2Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher2Stream =
                publisher2Client.publishBlockStream(publisher2Observer);

        // Expect 3 responses: RESEND(3), ACK(4), ACK(5).
        final AtomicReference<CountDownLatch> publisher2Latch = publisher2Observer.setAndGetOnNextLatch(3);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisher2Stream);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2)));
        endBlock(3L, publisher2Stream);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisher2Stream);

        awaitLatch(publisher2Latch, "RESEND(3) and ACKs for blocks 4 and 5");

        final List<PublishStreamResponse> publisher2Responses = publisher2Observer.getOnNextCalls();
        assertThat(publisher2Responses)
                .as("publisher 2 must receive RESEND(3) — gap was detected, publisher asked to fill it")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(3L, resendBlockNumberExtractor));
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block 4")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(4L, acknowledgementBlockNumberExtractor));
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block 5")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(5L, acknowledgementBlockNumberExtractor));

        // All three blocks must be in storage — the gap at block 3 was filled by the RESEND.
        // ACK(5) was received only after block 5 was persisted, so blocks 3 and 4 (persisted
        // before block 5 in the forwarder's messaging order) are also present.
        final BlockAccessServiceInterface.BlockAccessServiceClient blockAccessClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(3L).build())
                        .status())
                .as("block 3 must be present in storage — gap was filled by RESEND mechanism")
                .isEqualTo(BlockResponse.Code.SUCCESS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(4L).build())
                        .status())
                .as("block 4 must be present in storage")
                .isEqualTo(BlockResponse.Code.SUCCESS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(5L).build())
                        .status())
                .as("block 5 must be present in storage")
                .isEqualTo(BlockResponse.Code.SUCCESS);

        publisher1Client.close();
        publisher2Client.close();
        blockAccessClient.close();
    }

    /**
     * Reproduces the block gap caused by {@code clearObsoleteQueueItems} evicting a stalled
     * publisher's RESEND queue before its block proof arrives.
     *
     * <p>Publisher A streams blocks 0–1 (fully ACKed), then sends only the header for block 2
     * and stalls (simulating a stalled block). Publisher B streams blocks 3–5; when block 5
     * completes, stall detection fires (5 &gt; 2 + MAX_ADVANCE = 4), removes block 2 from
     * {@code queueByBlockMap}, adds 2 to {@code blocksToResend}, and returns
     * {@code RESEND_BLOCK(2)} to publisher B.
     *
     * <p>Publisher B's stream then re-queues block 2 in two separate batches:
     * <ol>
     *   <li>Batch 1 — header + round-header (no proof): re-enters block 2 into
     *       {@code queueByBlockMap} without a proof so it is eligible for eviction.
     *   <li>After waiting for blocks 3–5 to be persisted (i.e. after
     *       {@code clearObsoleteQueueItems} has run), Batch 2 — footer + block proof arrives.
     * </ol>
     *
     * <p>With the bug, {@code clearObsoleteQueueItems(3)} checks
     * {@code headMap(3, inclusive) = {2}} while block 2 has no proof → block 2 is evicted.
     * Batch 2 arrives to a block 2 that no longer exists → SKIP. Block 2 is permanently lost.
     *
     * <p>Both blocks must end up in storage after the fix. Without the fix, the assertion on
     * block 2 fails because the gap is never filled.
     */
    @Test
    @DisplayName("stall-detected RESEND queue evicted by clearObsoleteQueueItems leaves permanent block gap")
    void stalledPublisherResendQueueEvictedByObsoleteCleanupLeavesBlockGap() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);
        final Bytes hash5 = BlockItemBuilderUtils.computeBlockHash(5L, hash4);

        // Publisher A — streams blocks 0–1 fully (ACKed), then stalls mid-block-2 with a
        // header-only batch. Stall detection will terminate this connection with TIMEOUT.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisherAClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisherAObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisherAStream =
                publisherAClient.publishBlockStream(publisherAObserver);

        final Bytes[] prevHashesForA = {null, hash0};
        for (long blockNum = 0L; blockNum <= 1L; blockNum++) {
            final AtomicReference<CountDownLatch> ackLatch = publisherAObserver.setAndGetOnNextLatch(1);
            publisherAStream.onNext(buildPublishRequest(
                    BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNum, prevHashesForA[(int) blockNum])));
            endBlock(blockNum, publisherAStream);
            awaitLatch(ackLatch, "ACK for block " + blockNum + " from publisher A");
        }

        // Only the block-2 header — publisher A stalls here.
        publisherAStream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(2L)}));
        final AtomicReference<CountDownLatch> publisherATimeoutLatch =
                publisherAObserver.setAndGetConnectionEndedLatch(1);

        // Publisher B — fills the stall gap via RESEND.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisherBClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisherBObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisherBStream =
                publisherBClient.publishBlockStream(publisherBObserver);

        // Expect 4 responses: RESEND_BLOCK(2) + ACK(3) + ACK(4) + ACK(5).
        // These are set before sending so that no response is missed.
        final AtomicReference<CountDownLatch> resendAndPersistLatch = publisherBObserver.setAndGetOnNextLatch(4);

        // Blocks 3–5: stall fires when endOfBlock(5) is processed.
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2)));
        endBlock(3L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisherBStream);

        // Block 2 re-send — Batch 1: header + round-header only, NO proof.
        // Sent immediately after blocks 3–5 so the server re-queues block 2 before blocks 3–5
        // are persisted. clearObsoleteQueueItems will see block 2 without a proof and evict it.
        final BlockItem[] fullBlock2 = BlockItemBuilderUtils.createSimpleBlockWithNumber(2L, hash1);
        final BlockItem[] block2HeaderBatch = {fullBlock2[0], fullBlock2[1]};
        final BlockItem[] block2ProofBatch = {fullBlock2[2], fullBlock2[3]};
        publisherBStream.onNext(buildPublishRequest(block2HeaderBatch));

        // Wait until stall has fired (RESEND_BLOCK received) and blocks 3–5 are persisted.
        // By this point clearObsoleteQueueItems has run for blocks 3–5 and evicted block 2
        // (no proof was in the queue when those persistence events fired).
        awaitLatch(resendAndPersistLatch, "RESEND_BLOCK(2) and ACKs for blocks 3, 4, 5");
        awaitLatch(publisherATimeoutLatch, "publisher A TIMEOUT due to stall detection");

        // Block 2 re-send — Batch 2: footer + block proof. With the bug, block 2's entry was
        // already evicted from queueByBlockMap, so the manager returns SKIP and the proof is
        // dropped. Without the fix, block 2 is permanently lost.
        final AtomicReference<CountDownLatch> finalLatch = publisherBObserver.setAndGetOnNextLatch(2);
        publisherBStream.onNext(buildPublishRequest(block2ProofBatch));
        endBlock(2L, publisherBStream);

        // Block 6 — confirms the system continues to process blocks after the RESEND sequence.
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(6L, hash5)));
        endBlock(6L, publisherBStream);

        // With bug:  SKIP_BLOCK(2) + ACK(6) = 2 responses.
        // With fix:  ACK(2) + ACK(6)         = 2 responses.
        awaitLatch(finalLatch, "block-2 proof result and ACK for block 6");

        final List<PublishStreamResponse> publisherBResponses = publisherBObserver.getOnNextCalls();
        assertThat(publisherBResponses)
                .as("publisher B must receive RESEND_BLOCK(2) — stall was detected on publisher A")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(2L, resendBlockNumberExtractor));

        // Core assertion: block 2 must be in storage after the RESEND completes.
        // FAILS on buggy code because clearObsoleteQueueItems evicted the re-queued block 2
        // (no proof present) when block 3 was persisted, causing the proof batch to be SKIPped.
        final BlockAccessServiceInterface.BlockAccessServiceClient blockAccessClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(2L).build())
                        .status())
                .as("block 2 must be in storage — stall RESEND must fill the gap")
                .isEqualTo(BlockResponse.Code.SUCCESS);

        // Block 5 is the collateral-damage block: received complete while block 2 was stalled.
        // It must also reach storage; the gap at block 2 must not leave it permanently lost.
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(5L).build())
                        .status())
                .as("block 5 must be in storage — collateral complete block must not be lost")
                .isEqualTo(BlockResponse.Code.SUCCESS);

        publisherAClient.close();
        publisherBClient.close();
        blockAccessClient.close();
    }

    /**
     * Reproduces the protocol ordering violation where the publisher manager forwards blocks 3–5
     * to messaging before the stalled block 2 is re-delivered.
     *
     * <p>Protocol requirement (HIP-1081): a Block-Node MUST NOT acknowledge block M before it has
     * verified, persisted, and acknowledged block M-1.
     *
     * <p>Publisher A streams blocks 0–1 (ACKed), then sends only the header for block 2 and
     * stalls. Publisher B streams blocks 3–5; when block 5 completes, stall detection fires: block
     * 2 is removed from {@code queueByBlockMap}, added to {@code blocksToResend}, and
     * RESEND_BLOCK(2) is returned to publisher B.
     *
     * <p>With the bug, after the stall the {@code MessagingForwarderTask} skips the block-2 gap
     * by calling {@code firstEntry()} and immediately forwards blocks 3–5. Those blocks are
     * persisted and ACK(3), ACK(4), ACK(5) are sent to publisher B before block 2 has been
     * re-delivered. Publisher B then sends the full block 2 as its RESEND response, but the
     * forwarder has already moved past it — block 2 is never persisted and ACK(2) never arrives.
     *
     * <p>With the fix, the forwarder respects sequential ordering and waits for block 2 before
     * forwarding blocks 3–5. After publisher B delivers block 2, all four blocks (2, 3, 4, 5) are
     * persisted in order, and ACK(2) is sent before ACK(3).
     */
    @Test
    @DisplayName("stall RESEND must not cause acknowledgements for later blocks before the stalled block")
    void stalledPublisherMustNotAcknowledgeLaterBlocksBeforeStalledBlock() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);
        final Bytes hash5 = BlockItemBuilderUtils.computeBlockHash(5L, hash4);

        // Publisher A — streams blocks 0–1 (ACKed), then stalls mid-block-2 (header only).
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisherAClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisherAObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisherAStream =
                publisherAClient.publishBlockStream(publisherAObserver);

        final Bytes[] prevHashesForA = {null, hash0};
        for (long blockNum = 0L; blockNum <= 1L; blockNum++) {
            final AtomicReference<CountDownLatch> ackLatch = publisherAObserver.setAndGetOnNextLatch(1);
            publisherAStream.onNext(buildPublishRequest(
                    BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNum, prevHashesForA[(int) blockNum])));
            endBlock(blockNum, publisherAStream);
            awaitLatch(ackLatch, "ACK for block " + blockNum + " from publisher A");
        }
        // Publisher A stalls here — only the block-2 header is sent, no proof.
        publisherAStream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(2L)}));

        // Publisher B — streams blocks 3–5 to trigger stall detection.
        // Set latch(1) before sending so no response is missed.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisherBClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisherBObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisherBStream =
                publisherBClient.publishBlockStream(publisherBObserver);

        final AtomicReference<CountDownLatch> resendLatch = publisherBObserver.setAndGetOnNextLatch(1);

        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2)));
        endBlock(3L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisherBStream);

        // Wait until stall fires. RESEND_BLOCK(2) is sent synchronously in the handler thread
        // as part of endOfBlock(5) processing and is enqueued into the response pipeline before
        // any persistence ACKs (which travel through the asynchronous forwarder → disruptor →
        // persistence chain). The FIFO pipeline guarantees RESEND_BLOCK(2) arrives first.
        awaitLatch(resendLatch, "RESEND_BLOCK(2) from stall detection");

        // Expect 5 more responses: ACK(2) + ACK(3) + ACK(4) + ACK(5) + ACK(6).
        // With the bug, ACK(2) never arrives because the forwarder has already skipped past
        // block 2 and will not go back to forward it — this latch times out.
        final AtomicReference<CountDownLatch> remainingAckLatch = publisherBObserver.setAndGetOnNextLatch(5);

        // Publisher B provides the full block 2 as its RESEND response.
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(2L, hash1)));
        endBlock(2L, publisherBStream);

        // Block 6 confirms normal processing resumes after the RESEND sequence.
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(6L, hash5)));
        endBlock(6L, publisherBStream);

        awaitLatch(remainingAckLatch, "ACK(2), ACK(3), ACK(4), ACK(5), and ACK(6) from publisher B");

        final List<Long> ackBlockOrder = publisherBObserver.getOnNextCalls().stream()
                .filter(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT)
                .map(acknowledgementBlockNumberExtractor)
                .toList();

        // Core assertion: ACK(2) must be present and must precede all ACK(N > 2).
        // FAILS on buggy code because the forwarder skips the block-2 gap, forwards blocks 3–5
        // first, and block 2 is never persisted — ACK(2) is absent entirely.
        final int ack2Position = ackBlockOrder.indexOf(2L);
        assertThat(ack2Position)
                .as("publisher B must receive ACK for block 2 — stalled block must be acknowledged")
                .isGreaterThanOrEqualTo(0);
        assertThat(ackBlockOrder.subList(0, ack2Position))
                .as("no block with number greater than 2 may be acknowledged before the stalled block 2")
                .allSatisfy(blockNum -> assertThat(blockNum).isLessThanOrEqualTo(2L));

        publisherAClient.close();
        publisherBClient.close();
    }

    /**
     * Bug reproduction: asserts the current out-of-order acknowledgement behaviour produced by
     * {@code MessagingForwarderTask} skipping the stalled block-2 gap.
     *
     * <p><b>This test is expected to PASS on the current buggy code and FAIL once the bug is
     * fixed.</b> It documents the observed violation so the fix can be verified against it.
     *
     * <p>After stall detection removes block 2 from {@code queueByBlockMap}, the forwarder calls
     * {@code firstEntry()} and immediately forwards blocks 3–5, producing ACK(3), ACK(4), ACK(5)
     * before block 2 is re-delivered. Publisher B then sends block 2 as the RESEND response, but
     * the forwarder has already advanced its {@code lastForwardedBlockNumber} past block 2 — it
     * will never forward block 2. ACK(2) is therefore absent from publisher B's responses, while
     * ACK(3) and later are present — a direct protocol ordering violation.
     *
     * <p>With the fix the forwarder waits for block 2 before forwarding blocks 3–5, ACK(2)
     * arrives before ACK(3), and the {@code doesNotContain(2L)} assertion below fails.
     */
    @Test
    @DisplayName("[BUG] stalled forwarder skips block-2 gap and acknowledges later blocks out of order")
    void bugRepro_stalledForwarderSkipsGapAndAcknowledgesLaterBlocksOutOfOrder() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);
        final Bytes hash5 = BlockItemBuilderUtils.computeBlockHash(5L, hash4);

        // Publisher A — streams blocks 0–1 (ACKed), then stalls mid-block-2 (header only).
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisherAClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisherAObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisherAStream =
                publisherAClient.publishBlockStream(publisherAObserver);

        final Bytes[] prevHashesForA = {null, hash0};
        for (long blockNum = 0L; blockNum <= 1L; blockNum++) {
            final AtomicReference<CountDownLatch> ackLatch = publisherAObserver.setAndGetOnNextLatch(1);
            publisherAStream.onNext(buildPublishRequest(
                    BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNum, prevHashesForA[(int) blockNum])));
            endBlock(blockNum, publisherAStream);
            awaitLatch(ackLatch, "ACK for block " + blockNum + " from publisher A");
        }
        publisherAStream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(2L)}));

        // Publisher B — streams blocks 3–5 to trigger stall detection, then sends block 2 RESEND.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisherBClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisherBObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisherBStream =
                publisherBClient.publishBlockStream(publisherBObserver);

        // latch(1) fires on RESEND_BLOCK(2): it is sent synchronously in the handler thread during
        // endOfBlock(5) and enters the response pipeline before any persistence ACKs can, so the
        // pipeline FIFO ordering guarantees it arrives first.
        final AtomicReference<CountDownLatch> resendLatch = publisherBObserver.setAndGetOnNextLatch(1);

        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2)));
        endBlock(3L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisherBStream);

        awaitLatch(resendLatch, "RESEND_BLOCK(2) from stall detection");

        // With the bug the forwarder has begun forwarding blocks 3–5 asynchronously. Publisher B
        // now sends block 2 (full) and block 6. Block 2 is never forwarded (forwarder is past it);
        // block 6 is forwarded normally. Expect 4 responses: ACK(3) + ACK(4) + ACK(5) + ACK(6).
        //
        // With the fix the forwarder waited: publisher B's block 2 fills the gap, the forwarder
        // then forwards 2 → 3 → 4 → 5 in order, and ACK(2) arrives as the first of the 4
        // responses — which causes the assertion below to fail.
        final AtomicReference<CountDownLatch> postResendLatch = publisherBObserver.setAndGetOnNextLatch(4);

        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(2L, hash1)));
        endBlock(2L, publisherBStream);
        publisherBStream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(6L, hash5)));
        endBlock(6L, publisherBStream);

        awaitLatch(postResendLatch, "ACK(3), ACK(4), ACK(5), and ACK(6) from publisher B");

        final List<Long> ackBlockOrder = publisherBObserver.getOnNextCalls().stream()
                .filter(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT)
                .map(acknowledgementBlockNumberExtractor)
                .toList();

        // BUG: block 2 is never forwarded — the forwarder skipped it and will not go back.
        // PASSES now (ACK(2) absent); FAILS when fixed (ACK(2) present).
        assertThat(ackBlockOrder)
                .as("BUG: block 2 must not be acknowledged — forwarder skipped the gap and moved past it")
                .doesNotContain(2L);

        // BUG: later blocks were acknowledged despite the unsatisfied gap at block 2.
        // PASSES now (ACK(3–5) present); FAILS when fixed (reordering prevented).
        assertThat(ackBlockOrder)
                .as("BUG: blocks later than 2 must be acknowledged before block 2 is delivered — ordering violation")
                .containsAnyOf(3L, 4L, 5L);

        publisherAClient.close();
        publisherBClient.close();
    }

    private PbjGrpcClient createGrpcClient() {
        final Duration timeoutDuration = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + serverPort)
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(timeoutDuration)
                        .build()))
                .connectTimeout(timeoutDuration)
                .keepAlive(true)
                .build();
        final PbjGrpcClientConfig grpcConfig =
                new PbjGrpcClientConfig(timeoutDuration, tls, OPTIONS.authority(), OPTIONS.contentType());
        return new PbjGrpcClient(webClient, grpcConfig);
    }

    private PublishStreamRequest buildPublishRequest(final BlockItem[] blockItems) {
        return PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems).build())
                .build();
    }

    private void endBlock(final long blockNumber, final Pipeline<? super PublishStreamRequest> requestStream) {
        requestStream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String description)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for " + description);
    }
}
