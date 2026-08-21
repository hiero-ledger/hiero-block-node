// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.backfill;

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
import java.io.IOException;
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
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.base.client.BlockNodeClient;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * In-process API test verifying that the backfill client reaches a source Block Node that exposes
 * the subscribe and server-status services on different ports.
 *
 * <p>A single real {@link BlockNodeApp} is booted with the subscriber and server-status plugins on
 * dedicated, distinct ports (the other services stay on the default port). Blocks 0..2 are published
 * into it, then the real {@link BlockNodeClient} used by backfill is pointed at the source with
 * {@code subscribe_port} = subscriber port and {@code status_port} = server-status port (and {@code port}
 * left as a fallback that serves neither RPC). The test asserts that the server-status RPC (dialed on
 * {@code status_port}) reports the available range and that the subscribe RPC (dialed on
 * {@code subscribe_port}) fetches the blocks. This guards the regression where the server-status RPC was
 * dialed on the subscribe port.
 */
@Tag("api")
@DisplayName("Backfill From Multiport Source Tests")
@Timeout(60)
public class BackfillMultiportSourceTests {

    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final int LAST_BLOCK_NUMBER = 2;
    private static final int GRPC_TIMEOUT_MS = 30_000;
    private static final int MAX_INCOMING_BUFFER_SIZE = 10_485_760;
    private static final int MAX_PROTOBUF_MESSAGE_SIZE = 10_485_760;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);

    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    /** The base port other services keep; the split ports are derived from it. */
    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

    private BlockNodeApp app;

    /** Default constructor. */
    public BackfillMultiportSourceTests() {}

    @BeforeEach
    void beforeEach() throws IOException {
        final Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        // Clear any per-service port properties left over from a previous (possibly crashed) run so
        // they can't spill into this test's app before it sets its own.
        System.clearProperty("server.status.port");
        System.clearProperty("subscriber.port");

        // Provision the verifier with the roster that signs blocks built by BlockItemBuilderUtils.
        BlockItemBuilderUtils.provisionTssBootstrap();
    }

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BackfillMultiportSourceTests", "test teardown");
        }
        System.clearProperty("server.status.port");
        System.clearProperty("subscriber.port");
    }

    @Test
    @DisplayName("Backfill client fetches from a source with subscribe and serverStatus on different ports")
    void backfillClientFetchesFromSplitPortSource() throws Exception {
        final int defaultPort = Integer.parseInt(serverPort);
        final int serverStatusPort = defaultPort + 3;
        final int subscriberPort = defaultPort + 5;

        // Move only the two services the backfill client dials so subscribe and serverStatus differ.
        System.setProperty("server.status.port", Integer.toString(serverStatusPort));
        System.setProperty("subscriber.port", Integer.toString(subscriberPort));

        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        awaitRunning();

        publishBlocks(defaultPort);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1_500));
        // The backfill client: subscribe dials `subscribe_port`, serverStatus dials `status_port`.
        // `port` is only a fallback and points at the default port (which serves neither of these
        // two RPCs), so the test fails if either RPC falls back instead of using its dedicated port.
        final BlockNodeSourceConfig source = BlockNodeSourceConfig.newBuilder()
                .address("localhost")
                .port(defaultPort)
                .subscribePort(subscriberPort)
                .statusPort(serverStatusPort)
                .build();
        try (BlockNodeClient client = new BlockNodeClient(
                source, GRPC_TIMEOUT_MS, false, MAX_INCOMING_BUFFER_SIZE, MAX_PROTOBUF_MESSAGE_SIZE, null)) {
            // Server-status RPC must reach the dedicated status port and report the stored range.
            final ServerStatusDetailResponse status = client.getBlockNodeServiceClient()
                    .serverStatusDetail(ServerStatusRequest.newBuilder().build());
            assertThat(status.availableRanges()).anySatisfy(range -> {
                assertThat(range.rangeStart()).isEqualTo(0L);
                assertThat(range.rangeEnd()).isEqualTo((long) LAST_BLOCK_NUMBER);
            });

            // Subscribe RPC must reach the subscribe port and fetch every block in the range.
            final List<BlockUnparsed> blocks =
                    client.getBlockstreamSubscribeUnparsedClient().getBatchOfBlocks(0L, LAST_BLOCK_NUMBER);
            assertThat(blocks).hasSize(LAST_BLOCK_NUMBER + 1);
        }
    }

    /// Publishes blocks 0..{@link #LAST_BLOCK_NUMBER}, chained by block hash, on the given port and
    /// awaits an acknowledgement covering the last block. Acknowledgements are watermark-coalesced by
    /// the server (an ack for block N implicitly acks all blocks below N), so fewer acks than blocks
    /// may arrive; waiting for a fixed per-block count would flake (#3401).
    private void publishBlocks(final int port) throws InterruptedException {
        final BlockStreamPublishServiceClient publishClient =
                new BlockStreamPublishServiceClient(createGrpcClientForPort(port), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishClient.publishBlockStream(observer);

        final AtomicReference<CountDownLatch> ackLatch = observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && response.acknowledgement().blockNumber() >= LAST_BLOCK_NUMBER);
        Bytes previousBlockHash = null;
        for (long blockNumber = 0; blockNumber <= LAST_BLOCK_NUMBER; blockNumber++) {
            final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber, previousBlockHash);
            stream.onNext(PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                    .build());
            stream.onNext(PublishStreamRequest.newBuilder()
                    .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                    .build());
            previousBlockHash = BlockItemBuilderUtils.computeBlockHash(blockNumber, previousBlockHash);
        }

        ackLatch.get().await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(
                0,
                ackLatch.get().getCount(),
                "Timed out waiting for a publish acknowledgement of block >= " + LAST_BLOCK_NUMBER);
        stream.closeConnection();
        publishClient.close();
    }

    private void awaitRunning() throws InterruptedException {
        final long deadlineMs = System.currentTimeMillis() + 10_000L;
        while (app.blockNodeState() != State.RUNNING) {
            if (System.currentTimeMillis() >= deadlineMs) {
                assertEquals(State.RUNNING, app.blockNodeState(), "app did not reach RUNNING state");
            }
            Thread.sleep(20);
        }
    }

    private PbjGrpcClient createGrpcClientForPort(final int port) {
        final Duration timeout = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + port)
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(timeout)
                        .build()))
                .connectTimeout(timeout)
                .keepAlive(true)
                .build();
        final PbjGrpcClientConfig grpcConfig =
                new PbjGrpcClientConfig(timeout, tls, OPTIONS.authority(), OPTIONS.contentType());
        return new PbjGrpcClient(webClient, grpcConfig);
    }
}
