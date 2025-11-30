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
     * 2. Publishing a duplicate genesis block and confirming duplicate block response and stream closure
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

        // ==== Scenario 2: Publish duplicate genesis block and confirm duplicate block response and stream closure ===
        requestStream.onNext(request);

        // to-do: investigate occasional test failures here - revert to await on onComplete responseObserver latch
        parkNanos(2_000_000_000L);

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

        final AtomicReference<CountDownLatch> blockItemsSubscribe1Latch =
                subscribeResponseObserver.setAndGetOnNextLatch(2);
        blockStreamSubscribeServiceClient.subscribeBlockStream(subscribeRequest1, subscribeResponseObserver);

        awaitLatch(blockItemsSubscribe1Latch, "historical subscription");
        // block items, end block, and success status
        assertThat(subscribeResponseObserver.getOnNextCalls()).hasSize(3);
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
        BlockItem[] blockItems1 = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber1);
        PublishStreamRequest request2 = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems1).build())
                .build();

        // use a new client to publish block 1 as the existing client was closed on duplicate block publish.
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

        final SubscribeStreamResponse subscribeResponse1 =
                subscribeResponseObserver.getOnNextCalls().get(3);
        assertThat(subscribeResponse1.blockItems().blockItems()).hasSize(blockItems1.length);

        // 6 items expected: block 0 items, end block 0, success status, block 1 items, end block 1, success status
        assertThat(subscribeResponseObserver.getOnNextCalls()).element(3).satisfies(response -> {
            assertThat(response.blockItems().blockItems()).hasSize(blockItems1.length);
            assertThat(response.blockItems()
                            .blockItems()
                            .getFirst()
                            .blockHeader()
                            .number())
                    .isEqualTo(blockNumber1);
        });
        assertThat(subscribeResponseObserver
                        .getOnNextCalls()
                        .get(4)
                        .endOfBlock()
                        .blockNumber())
                .isEqualTo(blockNumber1);

        if (subscribeResponseObserver.getOnNextCalls().size() > 5) {
            // success status should be the last response
            assertThat(subscribeResponseObserver.getOnNextCalls().get(5).status())
                    .isEqualTo(SubscribeStreamResponse.Code.SUCCESS);
        }

        // close the client connections
        requestStream.closeConnection();
        requestStream2.closeConnection();
        awaitThread(subscribeThread, "live subscribe thread");
        blockStreamPublishServiceClient.close();
        blockStreamPublishServiceClient2.close();
        blockStreamSubscribeServiceClient.close();
        blockAccessServiceClient.close();
        blockNodeServiceClient.close();
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

        newRequestStream.onNext(swappedRequest);

        // sleep briefly to allow processing
        parkNanos(5_000_000_000L);

        // Assert that no responses have been sent.
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
     *   <li>Publishing a new genesis block and confirming acknowledgement response.</li>
     *   <li>Publishing a duplicate genesis block and confirming duplicate block response and stream closure.</li>
     *   <li>Attempting to publish a new block after the stream is closed, expecting an UncheckedIOException caused by a closed socket.</li>
     * </ol>
     */
    @Test
    void e2eSocketClosureTest() throws InterruptedException {
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
        requestStream.onNext(request);

        // to-do: investigate occasional test failures here - revert to await on onComplete responseObserver latch
        parkNanos(2_000_000_000L);

        // query status to confirm block range still shows only block 0
        BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient =
                new BlockNodeServiceInterface.BlockNodeServiceClient(serverStatusPbjGrpcClient, OPTIONS);
        final ServerStatusResponse nodeStatusPostDuplicateBlock =
                blockNodeServiceClient.serverStatus(SIMPLE_SERVER_STAUS_REQUEST);
        assertNotNull(nodeStatusPostDuplicateBlock);
        assertThat(nodeStatusPostDuplicateBlock.firstAvailableBlock()).isEqualTo(0);
        assertThat(nodeStatusPostDuplicateBlock.lastAvailableBlock()).isEqualTo(0);

        // ==== Scenario 3: Attempt to publish a new block after stream closure and expect UncheckedIOException ====
        final long blockNumber1 = 1;
        BlockItem[] blockItems1 = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber1);
        PublishStreamRequest request1 = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems1).build())
                .build();

        // This asserts that UncheckedIOException is thrown and its cause is a SocketException with "socket closed"
        UncheckedIOException ex = assertThrows(UncheckedIOException.class, () -> {
            // Expected exception to be thrown on the onNext() due to closed socket
            // from previous duplicate block publish
            requestStream.onNext(request1);
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

    // to-do: investigate CI related test failures where countdown latches don't hit zero.
    // Possibly due to thread contention.
    // Revert other tests that use parkNanos or don't check responseObserver to await on onNext or onComplete latches.
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
        AtomicReference<CountDownLatch> publishCompleteCountDownLatch = responseObserver.setAndGetOnCompleteLatch(1);
        requestStream.onNext(request);

        awaitLatch(
                publishCompleteCountDownLatch,
                "duplicate block end-of-stream onComplete"); // wait for onComplete caused by duplicate response

        // Assert that one more response is sent.
        assertThat(responseObserver.getOnNextCalls())
                .hasSize(2)
                .element(1)
                .returns(PublishStreamResponse.ResponseOneOfType.END_STREAM, responseKindExtractor)
                .returns(PublishStreamResponse.EndOfStream.Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                .returns(0L, endStreamBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(responseObserver.getOnErrorCalls()).isEmpty();
        assertThat(responseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(responseObserver.getOnCompleteCalls().get()).isEqualTo(1);
        assertThat(responseObserver.getClientEndStreamCalls().get()).isEqualTo(0);
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
