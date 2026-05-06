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
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(60);
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
            final long startupDeadline = System.currentTimeMillis() + 10_000;
            while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < startupDeadline) {
                Thread.sleep(10);
            }
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
     * <p>ACK delivery: ACK(3) is always sent. ACK(4) may be suppressed when
     * {@code handlePersisted(4)} fires before the forwarder removes block 4's key from
     * {@code queueByBlockMap} — {@code correctForResendAndStreaming(4)} returns 3 in that case.
     * ACK(5) is always sent because the forwarder removes block 5's key before dispatching
     * to messaging. A newer ACK implicitly acknowledges all prior blocks.
     *
     * <p>All three blocks must end up in storage. Without the fix, block 3's queue is never
     * forwarded to messaging and the gap persists silently.
     */
    @Test
    @DisplayName("publisher RESEND fills block gap left by severed connection")
    void publisherResendFillsBlockGapLeftBySeveredConnection() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);

        // Publisher 1: stream blocks 0–2 with proper hash chain, ACK for each.
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
        publisher1Stream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(3L)}));
        final AtomicReference<CountDownLatch> publisher1DisconnectLatch =
                publisher1Observer.setAndGetConnectionEndedLatch(1);
        publisher1Stream.onNext(PublishStreamRequest.newBuilder()
                .endStream(PublishStreamRequest.EndStream.newBuilder()
                        .endCode(PublishStreamRequest.EndStream.Code.RESET)
                        .build())
                .build());
        awaitLatch(publisher1DisconnectLatch, "publisher 1 disconnect after block 3 header");

        // Publisher 2: send block 4, then block 3 (the gap), then block 5.
        // endOfBlock(4) detects the gap and returns RESEND(3).
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher2Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher2Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher2Stream =
                publisher2Client.publishBlockStream(publisher2Observer);

        final AtomicReference<CountDownLatch> publisher2Latch = publisher2Observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && Objects.requireNonNull(response.acknowledgement()).blockNumber() == 5L);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisher2Stream);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2)));
        endBlock(3L, publisher2Stream);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisher2Stream);

        awaitLatch(publisher2Latch, "RESEND(3) and ACK for block 5");

        final List<PublishStreamResponse> publisher2Responses = publisher2Observer.getOnNextCalls();
        assertThat(publisher2Responses)
                .as("publisher 2 must receive RESEND(3) — gap was detected, publisher asked to fill it")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(3L, resendBlockNumberExtractor));
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block 5 — all blocks through 5 are persisted")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(5L, acknowledgementBlockNumberExtractor));

        final BlockAccessServiceInterface.BlockAccessServiceClient blockAccessClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        for (long blockNum = 3L; blockNum <= 5L; blockNum++) {
            assertThat(blockAccessClient
                            .getBlock(BlockRequest.newBuilder()
                                    .blockNumber(blockNum)
                                    .build())
                            .status())
                    .as("block %d must be present in storage — gap was filled by RESEND", blockNum)
                    .isEqualTo(BlockResponse.Code.SUCCESS);
        }

        publisher1Client.close();
        publisher2Client.close();
        blockAccessClient.close();
    }

    /**
     * Reproduces the stall-detection regression where a stalled publisher's block is never
     * acknowledged and later blocks are forwarded out of order.
     *
     * <p>Publisher 1 streams blocks 0–1 (fully ACKed), then sends only the header for block 2
     * and goes silent. Publisher 2 streams blocks 3–6; when block 6 completes, stall detection
     * fires (6 &gt; 2 + MaxFutureBlocksBeforeStalled = 5), removes block 2 from
     * {@code queueByBlockMap}, adds 2 to {@code blocksToResend}, and returns
     * {@code RESEND_BLOCK(2)} to publisher 2.
     *
     * <p>Publisher 2 responds to the RESEND by re-delivering the complete block 2, then
     * continues with blocks 7–8.
     *
     * <p>With the fix, the forwarder respects sequential ordering: block 2 is forwarded before
     * blocks 3–6, ACK(2) is sent before ACK(3), and all blocks reach storage without gaps.
     *
     * <p>Cross-connection synchronization: publisher 1's block 2 header and publisher 2's block 3
     * are on different gRPC connections with no guaranteed ordering. Publisher 2 retries block 3
     * until the server stops returning {@code NODE_BEHIND_PUBLISHER}, which proves publisher 1's
     * header was processed and {@code nextUnstreamedBlockNumber >= 3}.
     */
    @Test
    @DisplayName("stall-detected RESEND must persist stalled block and acknowledge in order")
    void stallDetectedResendMustPersistStalledBlockAndAcknowledgeInOrder() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);
        final Bytes hash5 = BlockItemBuilderUtils.computeBlockHash(5L, hash4);
        final Bytes hash6 = BlockItemBuilderUtils.computeBlockHash(6L, hash5);
        final Bytes hash7 = BlockItemBuilderUtils.computeBlockHash(7L, hash6);

        // Publisher 1: blocks 0–1 fully ACKed, then header-only block 2 (stall).
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher1Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher1Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher1Stream =
                publisher1Client.publishBlockStream(publisher1Observer);

        final Bytes[] previousHashes = {null, hash0};
        for (long blockNum = 0L; blockNum <= 1L; blockNum++) {
            final AtomicReference<CountDownLatch> ackLatch = publisher1Observer.setAndGetOnNextLatch(1);
            publisher1Stream.onNext(buildPublishRequest(
                    BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNum, previousHashes[(int) blockNum])));
            endBlock(blockNum, publisher1Stream);
            awaitLatch(ackLatch, "ACK for block " + blockNum + " from publisher 1");
        }
        publisher1Stream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(2L)}));

        // Publisher 2: sync, trigger stall detection, fill gap via RESEND.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher2Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher2Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher2Stream =
                publisher2Client.publishBlockStream(publisher2Observer);

        // Synchronize: ensure publisher 1's block 2 header was processed before
        // sending blocks that depend on nextUnstreamedBlockNumber >= 3.
        sendBlockUntilAccepted(
                publisher2Stream, publisher2Observer, BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2), 3L);

        // Blocks 4–6 trigger stall detection (6 > 2 + 3).
        final AtomicReference<CountDownLatch> resendLatch = publisher2Observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisher2Stream);
        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisher2Stream);
        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(6L, hash5)));
        endBlock(6L, publisher2Stream);

        awaitLatch(resendLatch, "RESEND_BLOCK(2) from stall detection", publisher2Observer);

        // Fill the gap: send complete block 2.
        final AtomicReference<CountDownLatch> ack2Latch = publisher2Observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && Objects.requireNonNull(response.acknowledgement()).blockNumber() >= 2L);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(2L, hash1)));
        endBlock(2L, publisher2Stream);

        awaitLatch(ack2Latch, "ACK(>=2) — block 2 persisted after RESEND");

        // Terminal signal: blocks 7–8 confirm the full chain is flowing.
        final AtomicReference<CountDownLatch> terminalLatch = publisher2Observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && Objects.requireNonNull(response.acknowledgement()).blockNumber() >= 7L);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(7L, hash6)));
        endBlock(7L, publisher2Stream);
        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(8L, hash7)));
        endBlock(8L, publisher2Stream);

        awaitLatch(terminalLatch, "ACK(>=7) — terminal signal", publisher2Observer);

        // Verify RESEND was issued for block 2.
        assertThat(publisher2Observer.getOnNextCalls())
                .as("publisher 2 must receive RESEND(2) — stall detection must request the gap block")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(2L, resendBlockNumberExtractor));

        // Verify all blocks 2–6 are in storage (no gaps).
        final BlockAccessServiceInterface.BlockAccessServiceClient blockAccessClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        for (long blockNum = 2L; blockNum <= 6L; blockNum++) {
            assertThat(blockAccessClient
                            .getBlock(BlockRequest.newBuilder()
                                    .blockNumber(blockNum)
                                    .build())
                            .status())
                    .as("block %d must be in storage — stall RESEND sequence must leave no gaps", blockNum)
                    .isEqualTo(BlockResponse.Code.SUCCESS);
        }

        // Verify ACK ordering: ACK(2) must arrive before any ACK for a higher block.
        final List<Long> ackBlockNumbers = publisher2Observer.getOnNextCalls().stream()
                .filter(response ->
                        response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT)
                .map(response ->
                        Objects.requireNonNull(response.acknowledgement()).blockNumber())
                .toList();

        assertThat(ackBlockNumbers).as("ACK(2) must be present").contains(2L);

        final int ack2Index = ackBlockNumbers.indexOf(2L);
        assertThat(ackBlockNumbers.subList(0, ack2Index))
                .as("no ACK for a block higher than 2 may arrive before ACK(2)")
                .allSatisfy(blockNumber -> assertThat(blockNumber).isLessThanOrEqualTo(2L));

        publisher1Client.close();
        publisher2Client.close();
        blockAccessClient.close();
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

    /**
     * Sends the given block repeatedly until the server accepts it (i.e. stops
     * returning {@code NODE_BEHIND_PUBLISHER}). {@code NODE_BEHIND_PUBLISHER}
     * is synchronous — if no response arrives within 500 ms the block was
     * accepted. The handler resets after each {@code BEHIND}, so retrying is
     * safe.
     */
    private void sendBlockUntilAccepted(
            final Pipeline<? super PublishStreamRequest> stream,
            final ResponsePipelineUtils<PublishStreamResponse> observer,
            final BlockItem[] blockItems,
            final long blockNumber)
            throws InterruptedException {
        boolean accepted = false;
        while (!accepted) {
            final int responsesBefore = observer.getOnNextCalls().size();
            final AtomicReference<CountDownLatch> latch = observer.setAndGetOnNextLatch(1);
            stream.onNext(buildPublishRequest(blockItems));
            endBlock(blockNumber, stream);
            final boolean responded = latch.get().await(500, TimeUnit.MILLISECONDS);
            if (!responded) {
                break;
            }
            final PublishStreamResponse response = observer.getOnNextCalls().get(responsesBefore);
            accepted = response.response().kind() != PublishStreamResponse.ResponseOneOfType.NODE_BEHIND_PUBLISHER;
        }
    }

    private void awaitLatch(
            final AtomicReference<CountDownLatch> latch,
            final String description,
            final ResponsePipelineUtils<PublishStreamResponse> observer)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (latch.get().getCount() != 0) {
            final StringBuilder diagnostics = new StringBuilder();
            diagnostics.append("Timed out waiting for ").append(description).append('\n');
            diagnostics
                    .append("Responses received (")
                    .append(observer.getOnNextCalls().size())
                    .append("):\n");
            for (int i = 0; i < observer.getOnNextCalls().size(); i++) {
                final PublishStreamResponse response = observer.getOnNextCalls().get(i);
                diagnostics
                        .append("  [")
                        .append(i)
                        .append("] ")
                        .append(response.response().kind());
                if (response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT) {
                    diagnostics
                            .append(" block=")
                            .append(Objects.requireNonNull(response.acknowledgement())
                                    .blockNumber());
                } else if (response.response().kind() == PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK) {
                    diagnostics
                            .append(" block=")
                            .append(Objects.requireNonNull(response.resendBlock())
                                    .blockNumber());
                } else if (response.response().kind()
                        == PublishStreamResponse.ResponseOneOfType.NODE_BEHIND_PUBLISHER) {
                    diagnostics
                            .append(" lastPersisted=")
                            .append(Objects.requireNonNull(response.nodeBehindPublisher())
                                    .blockNumber());
                } else if (response.response().kind() == PublishStreamResponse.ResponseOneOfType.SKIP_BLOCK) {
                    diagnostics
                            .append(" block=")
                            .append(Objects.requireNonNull(response.skipBlock()).blockNumber());
                }
                diagnostics.append('\n');
            }
            diagnostics
                    .append("Errors: ")
                    .append(observer.getOnErrorCalls().size())
                    .append('\n');
            for (final Throwable error : observer.getOnErrorCalls()) {
                diagnostics.append("  ").append(error).append('\n');
            }
            diagnostics
                    .append("onComplete calls: ")
                    .append(observer.getOnCompleteCalls().get());
            assertEquals(0, latch.get().getCount(), diagnostics.toString());
        }
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String description)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for " + description);
    }
}
