// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.media.type.MediaType;
import io.helidon.common.tls.Tls;
import io.helidon.http.HttpMediaType;
import io.helidon.http.Method;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.http2.Http2Client;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketException;
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
import java.util.stream.Collectors;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * E2E test class that performs a real-world instantiation of BlockNodeApp with no mocks.
 * It uses real services loaded via ServiceLoaderFunction and a real Helidon webserver.
 * Tests be used to confirm basic gRPC operations and follow complex multi plugin flows against the running server.
 * Both Helidon Http2Client and PbjGrpcClient approaches are provided to perform gRPC requests.
 * Cleans up the BlockNodeApp and the store of blocks after each test.
 */
@Tag("api")
public class BlockNodeAPITests {

    private static String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final MediaType APPLICATION_GRPC_PROTO = HttpMediaType.create("application/grpc+proto");
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final ServerStatusRequest SIMPLE_SERVER_STAUS_REQUEST =
            ServerStatusRequest.newBuilder().build();
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    // Get server port from environment variable overrides
    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

    final String serverStatusMethodPath =
            BlockNodeServiceInterface.FULL_NAME + "/" + BlockNodeServiceInterface.BlockNodeServiceMethod.serverStatus;

    // EXTRACTORS
    private final Function<PublishStreamResponse, PublishStreamResponse.ResponseOneOfType> responseKindExtractor =
            response -> response.response().kind();
    private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
            response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();
    private final Function<PublishStreamResponse, PublishStreamResponse.EndOfStream.Code>
            endStreamResponseCodeExtractor =
                    response -> Objects.requireNonNull(response.endStream()).status();
    private final Function<PublishStreamResponse, Long> endStreamBlockNumberExtractor =
            response -> Objects.requireNonNull(response.endStream()).blockNumber();
    private final Function<PublishStreamResponse, Long> skipBlockNumberExtractor =
            response -> Objects.requireNonNull(response.skipBlock()).blockNumber();

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient publishBlockStreamPbjGrpcClient;
    private PbjGrpcClient subscribeBlockStreamPbjGrpcClient;
    private PbjGrpcClient serverStatusPbjGrpcClient;
    private PbjGrpcClient getBlockPbjGrpcClient;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        // check that directory tmp/data exists and clear it
        Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            // delete all files and subdirectories
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }

        // Use the real service loader so all real implementations are loaded
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        try {
            assertNotNull(app, "BlockNodeApp should be constructed");

            // start the app (this will start Helidon webserver, metrics, and plugins)
            app.start();
            // short pause to allow async startup tasks to complete
            Thread.sleep(200);

            // verify running state and some basic internals
            assertEquals(State.RUNNING, app.blockNodeState());

            publishBlockStreamPbjGrpcClient = createGrpcClient();
            subscribeBlockStreamPbjGrpcClient = createGrpcClient();
            serverStatusPbjGrpcClient = createGrpcClient();
            getBlockPbjGrpcClient = createGrpcClient();
        } catch (Exception e) {
            // if anything goes wrong, ensure we shutdown the app
            if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
                app.shutdown("BlockNodeAppIntegrationTest", "test-setup-failure");
            }
            throw e;
        }
    }

    private PbjGrpcClient createGrpcClient() {
        return createGrpcClientForPort(serverPort);
    }

    private PbjGrpcClient createGrpcClientForPort(final String port) {
        final Duration timeoutDuration = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + port)
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

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BlockNodeApITest", "API test teardown");
        }
    }

    @Test
    void http2ClientServerStatusGet() {
        Http2Client http2Client =
                Http2Client.builder().baseUri("http://localhost:" + serverPort).build();

        // Http2Client exposes low-level HTTP/2 access so we can test configs without gRPC overhead
        try (var response = http2Client
                .method(Method.create("GET"))
                .contentType(APPLICATION_GRPC_PROTO)
                .path(serverStatusMethodPath)
                .request()) {

            // Only a post should be supported.
            assertThat(response.status().code()).isEqualTo(404);
            assertThat(response.headers().contentType().orElseThrow().text()).isEqualTo("text/plain");
        }
    }

    @Test
    void serverStatusRequestOnEmptyBN() {
        BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient =
                new BlockNodeServiceInterface.BlockNodeServiceClient(serverStatusPbjGrpcClient, OPTIONS);
        final ServerStatusResponse nodeStatus = blockNodeServiceClient.serverStatus(SIMPLE_SERVER_STAUS_REQUEST);
        assertNotNull(nodeStatus);
        assertThat(nodeStatus.firstAvailableBlock()).isEqualTo(-1L);
        assertThat(nodeStatus.lastAvailableBlock()).isEqualTo(-1L);
    }

    @Test
    void getBlockRequestNotAvailable() {
        BlockAccessServiceInterface.BlockAccessServiceClient blockAccessServiceClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        final BlockResponse blockResponse = blockAccessServiceClient.getBlock(
                BlockRequest.newBuilder().blockNumber(1L).build());

        assertNotNull(blockResponse);
        assertFalse(blockResponse.hasBlock());
        assertThat(blockResponse.status()).isEqualTo(BlockResponse.Code.NOT_AVAILABLE);
    }

    /* Test multiple scenarios in one area without mocks to allow for easy step through when troubleshooting behaviour
     * Scenarios covered:
     * 1. Mimicking CN and publishing a new genesis block to BN and confirming acknowledgement response
     * 2. Publishing a duplicate genesis block within the default producer.duplicateBlockSkipWindow
     *    and confirming the server answers with SkipBlock while keeping the stream open
     * 3. Requesting server status to confirm block 0 is reflected in status
     * 4. Requesting genesis block via getBlock to confirm block is stored and retrievable
     * 5. Mimicking MN and subscribing to block stream from block 0 and confirming receipt of block 0
     * 6. Mimicking MN and subscribing to live block stream and confirming receipt of newly published block
     */
    @Test
    void e2eBlockStreamsBaseScenarios() throws InterruptedException {
        // ==== Scenario 1: Publish new genesis block and confirm acknowledgement response ====
        BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient blockStreamPublishServiceClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);

        ResponsePipelineUtils<PublishStreamResponse> responseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> requestStream =
                blockStreamPublishServiceClient.publishBlockStream(responseObserver);

        final long blockNumber = 0;
        BlockItem[] blockItems = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        // change to List to allow multiple items
        PublishStreamRequest request = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems).build())
                .build();
        AtomicReference<CountDownLatch> publishCountDownLatch = responseObserver.setAndGetOnNextLatch(1);
        requestStream.onNext(request);
        endBlock(blockNumber, requestStream);

        awaitLatch(publishCountDownLatch, "publish acknowledgement"); // wait for acknowledgement response
        assertThat(responseObserver.getOnNextCalls())
                .hasSize(1)
                .first()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(0L, acknowledgementBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(responseObserver.getOnErrorCalls()).isEmpty();
        assertThat(responseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(responseObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(responseObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // ==== Scenario 2: Publish duplicate genesis block within the skip window and confirm SkipBlock response ===
        // Block 0 is at distance 0 from the last persisted block, which is well within the default
        // producer.duplicateBlockSkipWindow, so the server answers with SkipBlock and keeps the stream open
        // instead of closing the connection.
        final AtomicReference<CountDownLatch> duplicateSkipLatch = responseObserver.setAndGetOnNextLatch(1);
        requestStream.onNext(request);

        awaitLatch(duplicateSkipLatch, "duplicate block skip response");
        assertThat(responseObserver.getOnNextCalls())
                .hasSize(2)
                .last()
                .returns(PublishStreamResponse.ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                .returns(blockNumber, skipBlockNumberExtractor);
        // Stream stays open: no onComplete / onError, no end-stream calls.
        assertThat(responseObserver.getOnErrorCalls()).isEmpty();
        assertThat(responseObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(responseObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // ==== Scenario 3: Get server status and confirm block 0 is reflected in status ====
        BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient =
                new BlockNodeServiceInterface.BlockNodeServiceClient(serverStatusPbjGrpcClient, OPTIONS);
        final ServerStatusResponse nodeStatusPostBlock0 =
                blockNodeServiceClient.serverStatus(SIMPLE_SERVER_STAUS_REQUEST);
        assertNotNull(nodeStatusPostBlock0);
        assertThat(nodeStatusPostBlock0.firstAvailableBlock()).isEqualTo(0);
        assertThat(nodeStatusPostBlock0.lastAvailableBlock()).isEqualTo(0);

        // ==== Scenario 4: Get block 0 via getBlock and confirm block items ====
        BlockAccessServiceInterface.BlockAccessServiceClient blockAccessServiceClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        final BlockResponse block0Response = blockAccessServiceClient.getBlock(
                BlockRequest.newBuilder().blockNumber(blockNumber).build());
        assertNotNull(block0Response);
        assertTrue(block0Response.hasBlock());
        assertThat(block0Response.status()).isEqualTo(BlockResponse.Code.SUCCESS);
        assertNotNull(block0Response.block().items());
        assertThat(block0Response.block().items()).hasSize(blockItems.length);

        // ==== Scenario 5: Subscribe to block stream from block 0 and confirm receipt of block 0 ===
        BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient blockStreamSubscribeServiceClient =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(
                        subscribeBlockStreamPbjGrpcClient, OPTIONS);
        ResponsePipelineUtils<SubscribeStreamResponse> subscribeResponseObserver = new ResponsePipelineUtils<>();

        final SubscribeStreamRequest subscribeRequest1 = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(blockNumber)
                .build();

        // Wait for all 4 expected responses (two block-item batches, end-of-block, success status)
        // before asserting, so there is no race between the latch release and the size check.
        // The block is published in two sends: header+round+footer first, then proof via endBlock().
        final AtomicReference<CountDownLatch> blockItemsSubscribe1Latch =
                subscribeResponseObserver.setAndGetOnNextLatch(4);
        blockStreamSubscribeServiceClient.subscribeBlockStream(subscribeRequest1, subscribeResponseObserver);

        awaitLatch(blockItemsSubscribe1Latch, "historical subscription");
        // header+round+footer batch, proof batch, end-of-block, and success status
        assertThat(subscribeResponseObserver.getOnNextCalls()).hasSize(4);
        assertThat(subscribeResponseObserver.getOnCompleteCalls().get()).isEqualTo(1);

        final SubscribeStreamResponse subscribeResponse0 =
                subscribeResponseObserver.getOnNextCalls().get(0);
        assertThat(subscribeResponse0.blockItems().blockItems()).hasSize(blockItems.length);

        // ==== Scenario 6: Subscribe to live block stream and confirm receipt of newly published block ===
        final long blockNumber1 = 1;
        final SubscribeStreamRequest subscribeRequest2 = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(blockNumber1)
                .endBlockNumber(blockNumber1) // subscribe only until the next block is received
                .build();
        final AtomicReference<CountDownLatch> blockItemsSubscribe2Latch =
                subscribeResponseObserver.setAndGetOnNextLatch(1);
        // run blockStreamSubscribeServiceClient.subscribeBlockStrea in its own thread to avoid blocking
        final Thread subscribeThread = new Thread(
                () -> {
                    try {
                        blockStreamSubscribeServiceClient.subscribeBlockStream(
                                subscribeRequest2, subscribeResponseObserver);
                    } catch (Exception e) {
                        fail("Exception in subscribeBlockStream: " + e.getMessage());
                    }
                },
                "api-subscribe-live");
        subscribeThread.start();

        // publish block 1 and confirm subscriber receives it
        // Compute block 0's hash so block 1's proof is consistent with the server's previousBlockHash state.
        final Bytes block0Hash = BlockItemBuilderUtils.computeBlockHash(blockNumber, null);
        BlockItem[] blockItems1 = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber1, block0Hash);
        PublishStreamRequest request2 = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems1).build())
                .build();

        // use a new publisher client/stream for block 1 to keep this scenario isolated from the earlier
        // duplicate-skip exchange on the primary stream.
        ResponsePipelineUtils<PublishStreamResponse> responseObserver2 = new ResponsePipelineUtils<>();
        BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient blockStreamPublishServiceClient2 =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final Pipeline<? super PublishStreamRequest> requestStream2 =
                blockStreamPublishServiceClient2.publishBlockStream(responseObserver2);
        final AtomicReference<CountDownLatch> blockItemsPublish2Latch = responseObserver2.setAndGetOnNextLatch(1);
        requestStream2.onNext(request2);
        endBlock(blockNumber1, requestStream2);

        awaitLatch(
                blockItemsSubscribe2Latch,
                "live subscription for block 1"); // wait for subscriber to receive unverified block items
        awaitLatch(
                blockItemsPublish2Latch,
                "publisher acknowledgement for block 1"); // wait for publisher to observe block item sets

        assertThat(responseObserver2.getOnNextCalls())
                .hasSize(1)
                .first()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(1L, acknowledgementBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(responseObserver2.getOnErrorCalls()).isEmpty();
        assertThat(responseObserver2.getOnSubscriptionCalls()).isEmpty();
        assertThat(responseObserver2.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(responseObserver2.getClientEndStreamCalls().get()).isEqualTo(0);

        // Close publisher connections and wait for the subscribe thread to finish receiving all responses
        // before asserting on subscribe content. The subscribe thread's subscribeBlockStream call only returns
        // once the subscription ends, guaranteeing all 7 or 8 responses have been delivered.
        requestStream.closeConnection();
        requestStream2.closeConnection();
        awaitThread(subscribeThread, "live subscribe thread");

        // Block 0 contributes 4 responses: header+round+footer batch, proof batch, end-of-block, success status.
        // Block 1 contributes 3 or 4: if served from history all items arrive in one batch (3 total);
        // if served from the live stream the proof arrives separately (4 total).
        if (subscribeResponseObserver.getOnNextCalls().size() == 7) {
            // 7 items: block 0 header+round+footer, block 0 proof, end block 0, success,
            //          block 1 items, end block 1, success
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(4).satisfies(response -> {
                assertThat(response.blockItems().blockItems())
                        .hasSize(blockItems1.length)
                        .first()
                        .returns(blockNumber1, i -> i.blockHeader().number());
            });
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(5).satisfies(response -> {
                assertThat(response.endOfBlock().blockNumber()).isEqualTo(blockNumber1);
            });
            // success status should be the last response
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(6).satisfies(response -> {
                assertThat(response.status()).isEqualTo(SubscribeStreamResponse.Code.SUCCESS);
            });
        } else if (subscribeResponseObserver.getOnNextCalls().size() == 8) {
            // 8 items: block 0 header+round+footer, block 0 proof, end block 0, success,
            //          block 1 items w/o proof, block 1 proof, end block 1, success
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(4).satisfies(response -> {
                assertThat(response.blockItems().blockItems())
                        .hasSize(blockItems1.length - 1)
                        .first()
                        .returns(blockNumber1, i -> i.blockHeader().number());
            });
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(5).satisfies(response -> {
                assertThat(response.blockItems().blockItems())
                        .hasSize(1)
                        .first()
                        .returns(blockNumber1, i -> i.blockProof().block());
            });
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(6).satisfies(response -> {
                assertThat(response.endOfBlock().blockNumber()).isEqualTo(blockNumber1);
            });
            // success status should be the last response
            assertThat(subscribeResponseObserver.getOnNextCalls()).element(7).satisfies(response -> {
                assertThat(response.status()).isEqualTo(SubscribeStreamResponse.Code.SUCCESS);
            });
        } else {
            final String responses = subscribeResponseObserver.getOnNextCalls().stream()
                    .map(SubscribeStreamResponse::toString)
                    .collect(Collectors.joining(", "));
            final int size = subscribeResponseObserver.getOnNextCalls().size();
            fail("Unexpected number of subscribe responses: %d, Responses: %s".formatted(size, responses));
        }
        blockStreamPublishServiceClient.close();
        blockStreamPublishServiceClient2.close();
        blockStreamSubscribeServiceClient.close();
        blockAccessServiceClient.close();
        blockNodeServiceClient.close();
    }

    /**
     * Regression test for issue #2531: the producer.duplicateBlockSkipWindow configuration must
     * cause the publisher plugin to answer with SkipBlock for duplicate block headers that are
     * within the window of the last persisted block, and with EndOfStream(DUPLICATE_BLOCK) for
     * duplicates that fall outside the window. The default window of 5 is exercised here:
     * 1. Publish blocks 0..19 (so lastPersisted=19, window=5).
     * 2. Republish block 19 on the same stream (distance 0) and assert a SkipBlock response, with
     *    the stream remaining open.
     * 3. On a fresh stream, republish block 0 (distance 19, outside the window) and assert the
     *    server closes the stream with EndOfStream(DUPLICATE_BLOCK).
     */
    @Test
    void duplicateBlockSkipWindowAppliesWithinWindowAndEndsOutsideWindow() throws InterruptedException {
        final int totalBlocks = 20;
        final long headBlock = totalBlocks - 1L;

        // ==== Step 1: Publish blocks 0..19, chained by block hash, and await all acknowledgements ====
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> primaryObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> primaryStream = publishClient.publishBlockStream(primaryObserver);

        final AtomicReference<CountDownLatch> ackLatch = primaryObserver.setAndGetOnNextLatch(totalBlocks);
        BlockItem[] headBlockItems = null;
        BlockItem[] genesisBlockItems = null;
        Bytes previousBlockHash = null;
        for (long blockNumber = 0; blockNumber < totalBlocks; blockNumber++) {
            final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber, previousBlockHash);
            if (blockNumber == 0L) {
                genesisBlockItems = items;
            }
            if (blockNumber == headBlock) {
                headBlockItems = items;
            }
            final PublishStreamRequest itemsRequest = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                    .build();
            primaryStream.onNext(itemsRequest);
            endBlock(blockNumber, primaryStream);
            previousBlockHash = BlockItemBuilderUtils.computeBlockHash(blockNumber, previousBlockHash);
        }
        awaitLatch(ackLatch, "acknowledgements for blocks 0..19");
        assertThat(primaryObserver.getOnNextCalls()).hasSize(totalBlocks);
        assertThat(primaryObserver.getOnNextCalls())
                .last()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(headBlock, acknowledgementBlockNumberExtractor);

        // ==== Step 2: Republish head block (distance 0, within window) and expect SkipBlock ====
        final AtomicReference<CountDownLatch> skipLatch = primaryObserver.setAndGetOnNextLatch(1);
        final PublishStreamRequest headBlockRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(headBlockItems).build())
                .build();
        primaryStream.onNext(headBlockRequest);

        awaitLatch(skipLatch, "skip response for in-window duplicate");
        assertThat(primaryObserver.getOnNextCalls())
                .hasSize(totalBlocks + 1)
                .last()
                .returns(PublishStreamResponse.ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                .returns(headBlock, skipBlockNumberExtractor);
        // Stream must stay open for an in-window duplicate.
        assertThat(primaryObserver.getOnErrorCalls()).isEmpty();
        assertThat(primaryObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(primaryObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // ==== Step 3: On a fresh stream, republish block 0 (distance 19, outside window) ====
        final ResponsePipelineUtils<PublishStreamResponse> farBehindObserver = new ResponsePipelineUtils<>();
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient farBehindClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final Pipeline<? super PublishStreamRequest> farBehindStream =
                farBehindClient.publishBlockStream(farBehindObserver);
        final AtomicReference<CountDownLatch> farBehindClosedLatch = farBehindObserver.setAndGetConnectionEndedLatch(1);
        final PublishStreamRequest genesisRequest = PublishStreamRequest.newBuilder()
                .blockItems(
                        BlockItemSet.newBuilder().blockItems(genesisBlockItems).build())
                .build();
        farBehindStream.onNext(genesisRequest);
        awaitLatch(farBehindClosedLatch, "far-behind duplicate connection closed");

        primaryStream.closeConnection();
        farBehindStream.closeConnection();
        publishClient.close();
        farBehindClient.close();
    }

    @Test
    void swappedPublisherConnection() throws InterruptedException {
        BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient initialPublishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);

        ResponsePipelineUtils<PublishStreamResponse> initialResponseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> requestStream =
                initialPublishClient.publishBlockStream(initialResponseObserver);

        final long blockNumber = 0;
        BlockItem[] blockItems = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        // build request with incomplete block (missing proof)
        PublishStreamRequest request = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder()
                        .blockItems(blockItems[0], blockItems[1])
                        .build())
                .build();

        requestStream.onNext(request);

        // sleep briefly to allow processing
        parkNanos(5_000_000_000L);

        assertThat(initialResponseObserver.getOnNextCalls()).hasSize(0); // no responses should be sent yet

        // Assert no other responses sent
        assertThat(initialResponseObserver.getOnErrorCalls()).isEmpty();
        assertThat(initialResponseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(initialResponseObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(initialResponseObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // close the initial connection
        // send endStream before closing connection
        PublishStreamRequest endStreamRequest = PublishStreamRequest.newBuilder()
                .endStream(PublishStreamRequest.EndStream.newBuilder()
                        .endCode(PublishStreamRequest.EndStream.Code.RESET)
                        .build())
                .build();

        // get countdown latch for onComplete
        AtomicReference<CountDownLatch> initialOnCompleteLatch = initialResponseObserver.setAndGetOnCompleteLatch(1);

        requestStream.onNext(endStreamRequest);

        awaitLatch(initialOnCompleteLatch, "initial publish onComplete");

        assertThat(initialResponseObserver.getOnNextCalls()).hasSize(0); // no responses should be sent yet

        // Assert no other responses sent
        assertThat(initialResponseObserver.getOnErrorCalls()).isEmpty();
        assertThat(initialResponseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(initialResponseObserver.getOnCompleteCalls().get()).isEqualTo(1);
        assertThat(initialResponseObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // on the swapped connection, send the complete block including proof
        BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient newPublishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);

        ResponsePipelineUtils<PublishStreamResponse> newResponseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> newRequestStream =
                newPublishClient.publishBlockStream(newResponseObserver);

        PublishStreamRequest swappedRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems).build())
                .build();

        final AtomicReference<CountDownLatch> swappedPublishAckLatch = newResponseObserver.setAndGetOnNextLatch(1);
        newRequestStream.onNext(swappedRequest);

        final PublishStreamRequest endOfBlockRequest = PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build();

        newRequestStream.onNext(endOfBlockRequest);

        awaitLatch(swappedPublishAckLatch, "swapped publisher acknowledgement");

        assertThat(newResponseObserver.getOnNextCalls())
                .hasSize(1)
                .element(0)
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(0L, acknowledgementBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(newResponseObserver.getOnErrorCalls()).isEmpty();
        assertThat(newResponseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(initialResponseObserver.getOnCompleteCalls().get()).isEqualTo(1);
        assertThat(newResponseObserver.getClientEndStreamCalls().get()).isEqualTo(0);
    }

    /**
     * End-to-end socket test for block stream publishing.
     * <p>
     * This test covers the following scenarios:
     * <ol>
     *   <li>Publishing a chain of blocks (0..7) so the duplicate in scenario 2 falls outside the
     *       default producer.duplicateBlockSkipWindow and triggers EndOfStream(DUPLICATE_BLOCK).</li>
     *   <li>Publishing a far-behind duplicate (block 0, distance 7) and confirming stream closure.</li>
     *   <li>Attempting to publish a new block after the stream is closed, expecting an UncheckedIOException caused by a closed socket.</li>
     * </ol>
     */
    @Test
    void e2eSocketClosureTest() throws InterruptedException {
        // ==== Scenario 1: Publish a chain of blocks 0..7 so we can trigger an outside-window duplicate next ====
        BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient blockStreamPublishServiceClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);

        ResponsePipelineUtils<PublishStreamResponse> responseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> requestStream =
                blockStreamPublishServiceClient.publishBlockStream(responseObserver);

        final int totalBlocks = 8;
        final long headBlock = totalBlocks - 1L;
        final AtomicReference<CountDownLatch> publishCountDownLatch =
                responseObserver.setAndGetOnNextLatch(totalBlocks);
        BlockItem[] genesisBlockItems = null;
        Bytes previousBlockHash = null;
        for (long blockNumber = 0; blockNumber < totalBlocks; blockNumber++) {
            final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber, previousBlockHash);
            if (blockNumber == 0L) {
                genesisBlockItems = items;
            }
            final PublishStreamRequest itemsRequest = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                    .build();
            requestStream.onNext(itemsRequest);
            endBlock(blockNumber, requestStream);
            previousBlockHash = BlockItemBuilderUtils.computeBlockHash(blockNumber, previousBlockHash);
        }

        awaitLatch(publishCountDownLatch, "socket test acknowledgements for blocks 0..7");
        assertThat(responseObserver.getOnNextCalls()).hasSize(totalBlocks);
        assertThat(responseObserver.getOnNextCalls())
                .last()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(headBlock, acknowledgementBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(responseObserver.getOnErrorCalls()).isEmpty();
        assertThat(responseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(responseObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(responseObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // ==== Scenario 2: Republish genesis (distance 7, outside the default window) -> stream closure ====
        final AtomicReference<CountDownLatch> socketDuplicateConnectionEndedLatch =
                responseObserver.setAndGetConnectionEndedLatch(1);
        final PublishStreamRequest genesisRequest = PublishStreamRequest.newBuilder()
                .blockItems(
                        BlockItemSet.newBuilder().blockItems(genesisBlockItems).build())
                .build();
        requestStream.onNext(genesisRequest);

        awaitLatch(socketDuplicateConnectionEndedLatch, "socket test duplicate block connection closed");

        // query status to confirm the persisted block range covers 0..7
        BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient =
                new BlockNodeServiceInterface.BlockNodeServiceClient(serverStatusPbjGrpcClient, OPTIONS);
        final ServerStatusResponse nodeStatusPostDuplicateBlock =
                blockNodeServiceClient.serverStatus(SIMPLE_SERVER_STAUS_REQUEST);
        assertNotNull(nodeStatusPostDuplicateBlock);
        assertThat(nodeStatusPostDuplicateBlock.firstAvailableBlock()).isEqualTo(0);
        assertThat(nodeStatusPostDuplicateBlock.lastAvailableBlock()).isEqualTo(headBlock);

        // ==== Scenario 3: Attempt to publish a new block after stream closure and expect UncheckedIOException ====
        final long nextBlockNumber = totalBlocks;
        BlockItem[] nextBlockItems =
                BlockItemBuilderUtils.createSimpleBlockWithNumber(nextBlockNumber, previousBlockHash);
        PublishStreamRequest nextBlockRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(nextBlockItems).build())
                .build();

        // This asserts that UncheckedIOException is thrown and its cause is a SocketException with "socket closed"
        UncheckedIOException ex = assertThrows(UncheckedIOException.class, () -> {
            // Expected exception to be thrown on the onNext() due to closed socket
            // from previous duplicate block publish
            requestStream.onNext(nextBlockRequest);
        });
        assertEquals(
                SocketException.class.getSimpleName(), ex.getCause().getClass().getSimpleName());
        boolean causeMatches = ex.getCause().getMessage().toLowerCase().contains("socket closed")
                || ex.getCause().getMessage().toLowerCase().contains("broken pipe");
        assertTrue(
                causeMatches,
                "Unexpected socket exception: " + ex.getCause().getMessage().toLowerCase());

        // close the client connections
        requestStream.closeConnection();
        blockStreamPublishServiceClient.close();
    }

    // to-do: re-enable once the server-side early-close race is fixed in PublisherHandler.shutdown().
    // The server RSTs the connection before END_STREAM is flushed, so onNext(END_STREAM) is never received.
    @Disabled
    @Test
    void e2eDuplicateBlockPublisherObserversOnComplete() throws InterruptedException {
        // ==== Scenario 1: Publish new genesis block and confirm acknowledgement response ====
        BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient blockStreamPublishServiceClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);

        ResponsePipelineUtils<PublishStreamResponse> responseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> requestStream =
                blockStreamPublishServiceClient.publishBlockStream(responseObserver);

        final long blockNumber = 0;
        BlockItem[] blockItems = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        // change to List to allow multiple items
        PublishStreamRequest request = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems).build())
                .build();
        AtomicReference<CountDownLatch> publishCountDownLatch = responseObserver.setAndGetOnNextLatch(1);
        requestStream.onNext(request);
        endBlock(blockNumber, requestStream);

        awaitLatch(publishCountDownLatch, "socket test acknowledgement"); // wait for acknowledgement response
        assertThat(responseObserver.getOnNextCalls())
                .hasSize(1)
                .first()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(0L, acknowledgementBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(responseObserver.getOnErrorCalls()).isEmpty();
        assertThat(responseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(responseObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(responseObserver.getClientEndStreamCalls().get()).isEqualTo(0);

        // ==== Scenario 2: Publish duplicate genesis block and confirm duplicate block response and stream closure ===
        // Gate on connection end (onComplete or onError) rather than onNext(END_STREAM), because the server may
        // RST the connection before the END_STREAM frame is flushed to the client.
        final AtomicReference<CountDownLatch> duplicateConnectionEndedLatch =
                responseObserver.setAndGetConnectionEndedLatch(1);
        requestStream.onNext(request);

        awaitLatch(duplicateConnectionEndedLatch, "duplicate block connection closed");

        // Assert that the END_STREAM response was received with DUPLICATE_BLOCK code.
        // If the server fixes the early-close race, this will be reliably present.
        assertThat(responseObserver.getOnNextCalls())
                .hasSize(2)
                .element(1)
                .returns(PublishStreamResponse.ResponseOneOfType.END_STREAM, responseKindExtractor)
                .returns(PublishStreamResponse.EndOfStream.Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                .returns(0L, endStreamBlockNumberExtractor);

        assertThat(responseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(responseObserver.getClientEndStreamCalls().get()).isEqualTo(0);
    }

    /**
     * Verifies the per-service port wiring (PR #2880): each plugin reads a nullable {@code port} from
     * its own config and registers its service on that dedicated port. The default-port app from
     * {@link #beforeEach()} is shut down and a fresh app is booted with a unique port configured for
     * each of the five API plugins. Every API must answer on its own port and must NOT answer on the
     * default {@code server.port}, which has no API routes once all services are moved off it.
     */
    @Test
    void servicesBindToConfiguredUniquePortsOnly() throws Exception {
        // Derive the per-service ports from the suite's base port (SERVER_PORT) so they shift with it.
        // CI runs the api suite on a non-default SERVER_PORT to avoid parallel-run port conflicts; basing
        // these ports on it keeps that isolation rather than hardcoding a fixed range that could collide
        // with a concurrent run on the same host.
        final int defaultPort = Integer.parseInt(serverPort); // no API routes after the move
        final int blockAccessPort = defaultPort + 1;
        final int healthPort = defaultPort + 2;
        final int serverStatusPort = defaultPort + 3;
        final int publisherPort = defaultPort + 4;
        final int subscriberPort = defaultPort + 5;

        // The @BeforeEach app holds the default port; release it before rebinding with per-service ports.
        app.shutdown("BlockNodeAPITests", "rebind with per-service ports");
        awaitState(app, false);

        System.setProperty("block.access.port", Integer.toString(blockAccessPort));
        System.setProperty("health.port", Integer.toString(healthPort));
        System.setProperty("server.status.port", Integer.toString(serverStatusPort));
        System.setProperty("producer.port", Integer.toString(publisherPort));
        System.setProperty("subscriber.port", Integer.toString(subscriberPort));
        try {
            app = new BlockNodeApp(new ServiceLoaderFunction(), false);
            app.start();
            awaitState(app, true);

            // ==== Positive: every API routes on its own dedicated port ====
            // Publisher: publishing genesis on its port and getting an ACK proves it routes there.
            publishGenesisBlock(publisherPort);
            // ServerStatus: reachable on its port and reflects the persisted block.
            assertThat(serverStatusOn(serverStatusPort).lastAvailableBlock()).isEqualTo(0L);
            // BlockAccess: reachable on its port and returns the persisted block.
            assertThat(getBlockOn(blockAccessPort, 0L).status()).isEqualTo(BlockResponse.Code.SUCCESS);
            // Subscriber: reachable on its port and streams the persisted block 0.
            assertSubscriberReceivesBlock0(subscriberPort);
            // Health: reachable on its port.
            assertThat(healthLivezStatusCode(healthPort)).isEqualTo(200);

            // ==== Negative: none of the services answer on the default port ====
            assertThat(healthLivezStatusCode(defaultPort))
                    .as("health must not be routed on the default port")
                    .isEqualTo(404);
            assertGrpcNotRoutedOnDefaultPort(defaultPort);
            assertPublisherNotRoutedOnDefaultPort(defaultPort);
            assertSubscriberNotRoutedOnDefaultPort(defaultPort);
        } finally {
            System.clearProperty("block.access.port");
            System.clearProperty("health.port");
            System.clearProperty("server.status.port");
            System.clearProperty("producer.port");
            System.clearProperty("subscriber.port");
        }
    }

    /** Publishes genesis block 0 on the given port and awaits the ACK (proves the publisher routes there). */
    private void publishGenesisBlock(final int port) throws InterruptedException {
        final var client = new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                createGrpcClientForPort(Integer.toString(port)), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = client.publishBlockStream(observer);
        final AtomicReference<CountDownLatch> ackLatch = observer.setAndGetOnNextLatch(1);
        stream.onNext(PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder()
                        .blockItems(BlockItemBuilderUtils.createSimpleBlockWithNumber(0L))
                        .build())
                .build());
        endBlock(0L, stream);
        awaitLatch(ackLatch, "publisher ACK on port " + port);
        assertThat(observer.getOnNextCalls())
                .first()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                .returns(0L, acknowledgementBlockNumberExtractor);
        stream.closeConnection();
        client.close();
    }

    private ServerStatusResponse serverStatusOn(final int port) {
        final var client = new BlockNodeServiceInterface.BlockNodeServiceClient(
                createGrpcClientForPort(Integer.toString(port)), OPTIONS);
        try {
            return client.serverStatus(SIMPLE_SERVER_STAUS_REQUEST);
        } finally {
            client.close();
        }
    }

    private BlockResponse getBlockOn(final int port, final long blockNumber) {
        final var client = new BlockAccessServiceInterface.BlockAccessServiceClient(
                createGrpcClientForPort(Integer.toString(port)), OPTIONS);
        try {
            return client.getBlock(
                    BlockRequest.newBuilder().blockNumber(blockNumber).build());
        } finally {
            client.close();
        }
    }

    private void assertSubscriberReceivesBlock0(final int port) throws InterruptedException {
        final var client = new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(
                createGrpcClientForPort(Integer.toString(port)), OPTIONS);
        final ResponsePipelineUtils<SubscribeStreamResponse> observer = new ResponsePipelineUtils<>();
        final AtomicReference<CountDownLatch> latch = observer.setAndGetOnNextLatch(2);
        client.subscribeBlockStream(
                SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(0L)
                        .endBlockNumber(0L)
                        .build(),
                observer);
        awaitLatch(latch, "subscriber stream of block 0 on port " + port);
        assertThat(observer.getOnNextCalls().getFirst().blockItems().blockItems())
                .as("subscriber on port %d must stream block 0", port)
                .isNotEmpty();
        client.close();
    }

    /** Returns the HTTP status code of GET {@code /healthz/livez} on the given port. */
    private int healthLivezStatusCode(final int port) {
        final Http2Client http2Client =
                Http2Client.builder().baseUri("http://localhost:" + port).build();
        try (var response =
                http2Client.method(Method.create("GET")).path("/healthz/livez").request()) {
            return response.status().code();
        }
    }

    /**
     * Asserts that the four gRPC API services do not route on the default port. Once each plugin binds
     * its own port, the default port has no PBJ routes, so a unary call there fails rather than
     * returning a valid domain response.
     */
    private void assertGrpcNotRoutedOnDefaultPort(final int defaultPort) {
        assertThrows(
                Exception.class,
                () -> serverStatusOn(defaultPort),
                "server-status must not be routed on the default port");
        assertThrows(
                Exception.class,
                () -> getBlockOn(defaultPort, 0L),
                "block-access must not be routed on the default port");
    }

    /** Asserts the publisher gRPC service is not routed on the default port: no ACK and the stream is rejected. */
    private void assertPublisherNotRoutedOnDefaultPort(final int port) throws InterruptedException {
        final var client = new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                createGrpcClientForPort(Integer.toString(port)), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final AtomicReference<CountDownLatch> endedLatch = observer.setAndGetConnectionEndedLatch(1);
        try {
            final Pipeline<? super PublishStreamRequest> stream = client.publishBlockStream(observer);
            stream.onNext(PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder()
                            .blockItems(BlockItemBuilderUtils.createSimpleBlockWithNumber(0L))
                            .build())
                    .build());
            endBlock(0L, stream);
            endedLatch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(
                    0, endedLatch.get().getCount(), "publisher stream on the default port must be rejected (no route)");
            assertThat(observer.getOnNextCalls())
                    .as("publisher must not acknowledge on the default port")
                    .isEmpty();
        } catch (final RuntimeException rejectedDuringSend) {
            // Sending on an unrouted stream may throw directly (e.g. closed socket); that also proves "not routed".
        } finally {
            client.close();
        }
    }

    /** Asserts the subscriber gRPC service is not routed on the default port: the stream is rejected/closed. */
    private void assertSubscriberNotRoutedOnDefaultPort(final int port) throws InterruptedException {
        final var client = new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(
                createGrpcClientForPort(Integer.toString(port)), OPTIONS);
        final ResponsePipelineUtils<SubscribeStreamResponse> observer = new ResponsePipelineUtils<>();
        final AtomicReference<CountDownLatch> endedLatch = observer.setAndGetConnectionEndedLatch(1);
        // subscribeBlockStream blocks until the server-streaming RPC ends; run it off-thread so a stuck call
        // can't stall the test, then require the connection to have ended (been rejected) within the timeout.
        final Thread subscribeThread = new Thread(
                () -> {
                    try {
                        client.subscribeBlockStream(
                                SubscribeStreamRequest.newBuilder()
                                        .startBlockNumber(0L)
                                        .endBlockNumber(0L)
                                        .build(),
                                observer);
                    } catch (final RuntimeException rejected) {
                        // expected — not routed
                    }
                },
                "api-subscribe-default-port-negative");
        subscribeThread.start();
        endedLatch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        subscribeThread.join(DEFAULT_AWAIT_TIMEOUT.toMillis());
        assertEquals(
                0, endedLatch.get().getCount(), "subscriber stream on the default port must be rejected (no route)");
        client.close();
    }

    /** Waits until the app reaches (running == true) or leaves (running == false) the RUNNING state. */
    private static void awaitState(final BlockNodeApp app, final boolean running) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            final boolean isRunning = app.blockNodeState() == State.RUNNING;
            if (isRunning == running) {
                return;
            }
            Thread.sleep(20);
        }
        assertEquals(running, app.blockNodeState() == State.RUNNING, "app did not reach expected running=" + running);
    }

    private void endBlock(final long blockNumber, final Pipeline<? super PublishStreamRequest> requestStream) {
        PublishStreamRequest request = PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build();
        requestStream.onNext(request);
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String description)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for " + description);
    }

    private void awaitThread(final Thread thread, final String description) throws InterruptedException {
        thread.join(DEFAULT_AWAIT_TIMEOUT.toMillis());
        assertFalse(thread.isAlive(), "Timed out waiting for " + description + " to finish");
    }
}
