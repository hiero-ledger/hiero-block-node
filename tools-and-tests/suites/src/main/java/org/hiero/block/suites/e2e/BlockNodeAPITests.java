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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.hiero.block.api.*;
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
    private static final ServerStatusRequest SIMPLE_SERVER_STAUS_REQUEST =
            ServerStatusRequest.newBuilder().build();
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    // Get server port from environment variable overrides
    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

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
    void http2ClientPublishBlockStreamGet() {
        Http2Client http2Client =
                Http2Client.builder().baseUri("http://localhost:" + serverPort).build();

        // Http2Client exposes low-level HTTP/2 access so we can test configs without gRPC overhead
        try (var response = http2Client
                .method(Method.create("GET"))
                .contentType(APPLICATION_GRPC_PROTO)
                .path("/org.hiero.block.api.BlockNodeService/serverStatus")
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
        CountDownLatch publishCountDownLatch = responseObserver.setAndGetOnNextLatch(1);
        requestStream.onNext(request);

        publishCountDownLatch.await(); // wait for acknowledgement response
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
        CountDownLatch publishCompleteCountDownLatch = responseObserver.setAndGetOnCompleteLatch(1);
        requestStream.onNext(request);

        publishCompleteCountDownLatch.await(); // wait for onComplete caused by duplicate response

        // Assert that no responses have been sent.
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

        final CountDownLatch blockItemsSubscribe1Latch = subscribeResponseObserver.setAndGetOnNextLatch(2);
        blockStreamSubscribeServiceClient.subscribeBlockStream(subscribeRequest1, subscribeResponseObserver);

        blockItemsSubscribe1Latch.await();
        assertThat(subscribeResponseObserver.getOnNextCalls()).hasSize(2); // block items & success status
        assertThat(subscribeResponseObserver.getOnCompleteCalls().get()).isEqualTo(1);

        final SubscribeStreamResponse subscribeResponse0 =
                subscribeResponseObserver.getOnNextCalls().get(0);
        assertThat(subscribeResponse0.blockItems().blockItems()).hasSize(blockItems.length);

        // ==== Scenario 6: Subscribe to live block stream and confirm receipt of newly published block ===
        final SubscribeStreamRequest subscribeRequest2 = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(1L)
                .endBlockNumber(-1L) // subscribe to all future blocks
                .build();
        final CountDownLatch blockItemsSubscribe2Latch = subscribeResponseObserver.setAndGetOnNextLatch(1);
        // run blockStreamSubscribeServiceClient.subscribeBlockStrea in its own thread to avoid blocking
        new Thread(() -> {
                    try {
                        blockStreamSubscribeServiceClient.subscribeBlockStream(
                                subscribeRequest2, subscribeResponseObserver);
                    } catch (Exception e) {
                        fail("Exception in subscribeBlockStream: " + e.getMessage());
                    }
                })
                .start();

        // publish block 1 and confirm subscriber receives it
        final long blockNumber1 = 1;
        BlockItem[] blockItems1 = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber1);
        PublishStreamRequest request2 = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems1).build())
                .build();

        // use a new client to publish block 1 as the existing client was closed on duplicate block publish.
        ResponsePipelineUtils<PublishStreamResponse> responseObserver2 = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> requestStream2 =
                blockStreamPublishServiceClient.publishBlockStream(responseObserver2);
        final CountDownLatch blockItemsPublish2Latch = responseObserver2.setAndGetOnNextLatch(1);
        requestStream2.onNext(request2);

        blockItemsSubscribe2Latch.await(); // wait for subscriber to receive unverified block items
        blockItemsPublish2Latch.await(); // wait for publisher to observe block item sets

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
                subscribeResponseObserver.getOnNextCalls().get(2);
        assertThat(subscribeResponse1.blockItems().blockItems()).hasSize(blockItems1.length);

        assertThat(subscribeResponseObserver.getOnNextCalls())
                .hasSize(3) // block 0 items, success status and block 1 items
                .element(2)
                .satisfies(response -> {
                    assertThat(response.blockItems().blockItems()).hasSize(blockItems1.length);
                    assertThat(response.blockItems()
                                    .blockItems()
                                    .getFirst()
                                    .blockHeader()
                                    .number())
                            .isEqualTo(blockNumber1);
                });

        // close the client connections
        blockStreamPublishServiceClient.close();
        blockStreamSubscribeServiceClient.close();
        blockAccessServiceClient.close();
        blockNodeServiceClient.close();
    }

    @Disabled
    @Test
    void swappedPublisherConnection() {
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
        publishBlockStreamPbjGrpcClient.close();

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
                .returns(1L, acknowledgementBlockNumberExtractor);

        // Assert no other responses sent
        assertThat(newResponseObserver.getOnErrorCalls()).isEmpty();
        assertThat(newResponseObserver.getOnSubscriptionCalls()).isEmpty();
        assertThat(initialResponseObserver.getOnCompleteCalls().get()).isEqualTo(0);
        assertThat(newResponseObserver.getClientEndStreamCalls().get()).isEqualTo(0);
    }
}
