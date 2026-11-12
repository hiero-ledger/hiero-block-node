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
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
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
 * In-process API test of the backfill client against a source Block Node that holds only part of
 * the requested range.
 * <p>
 * The source has no live stream, so its subscriber session waits for the missing blocks and sends
 * nothing. The source's idle session limit then ends the stream with {@code SUCCESS}. The backfill
 * client must return the blocks it received instead of waiting for its own gRPC timeout. The
 * backfill fetcher treats that short batch as a failed fetch, which {@code BackfillFetcherTest}
 * covers.
 */
@Tag("api")
@DisplayName("Backfill From Partial Range Source Tests")
@Timeout(60)
public class BackfillPartialRangeSourceTests {
    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final String IDLE_TIMEOUT_PROPERTY = "subscriber.idleSessionTimeoutSeconds";
    private static final String IDLE_TIMEOUT_SECONDS = "2";
    private static final long LAST_PUBLISHED_BLOCK = 4L;
    private static final long LAST_REQUESTED_BLOCK = 9L;
    /** Longer than the source's idle session limit, so only the idle close can end the fetch in time. */
    private static final int GRPC_TIMEOUT_MS = 30_000;

    private static final int MAX_INCOMING_BUFFER_SIZE = 10_485_760;
    private static final int MAX_PROTOBUF_MESSAGE_SIZE = 10_485_760;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private final int serverPort =
            Integer.parseInt(System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT"));

    private BlockNodeApp app;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        final Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            try (Stream<Path> paths = Files.walk(dataDir)) {
                paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
        BlockItemBuilderUtils.provisionTssBootstrap();
        System.setProperty(IDLE_TIMEOUT_PROPERTY, IDLE_TIMEOUT_SECONDS);
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        assertThat(app.blockNodeState()).isEqualTo(State.RUNNING);
    }

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BackfillPartialRangeSourceTests", "API test teardown");
        }
        System.clearProperty(IDLE_TIMEOUT_PROPERTY);
    }

    @Test
    @DisplayName("Backfill client returns the available blocks when the source idle-closes a partial range")
    void backfillClientReturnsAvailableBlocksWhenSourceIdleCloses() throws Exception {
        publishBlocks();
        final BlockNodeSourceConfig source = BlockNodeSourceConfig.newBuilder()
                .address("localhost")
                .port(serverPort)
                .build();

        final List<BlockUnparsed> blocks;
        try (BlockNodeClient client = new BlockNodeClient(
                source, GRPC_TIMEOUT_MS, false, MAX_INCOMING_BUFFER_SIZE, MAX_PROTOBUF_MESSAGE_SIZE, null)) {
            blocks = client.getBlockstreamSubscribeUnparsedClient().getBatchOfBlocks(0L, LAST_REQUESTED_BLOCK);
        }

        assertThat(blocks).hasSize((int) LAST_PUBLISHED_BLOCK + 1);
    }

    /** Publishes blocks 0..{@link #LAST_PUBLISHED_BLOCK}, chained by block hash, and awaits their acknowledgement. */
    private void publishBlocks() throws InterruptedException, NoSuchAlgorithmException {
        final BlockStreamPublishServiceClient publishClient =
                new BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishClient.publishBlockStream(observer);
        final AtomicReference<CountDownLatch> acknowledged = observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && response.acknowledgement().blockNumber() >= LAST_PUBLISHED_BLOCK);
        Bytes previousBlockHash = null;
        for (long blockNumber = 0L; blockNumber <= LAST_PUBLISHED_BLOCK; blockNumber++) {
            final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber, previousBlockHash);
            stream.onNext(PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                    .build());
            stream.onNext(PublishStreamRequest.newBuilder()
                    .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                    .build());
            previousBlockHash = BlockItemBuilderUtils.computeBlockHash(blockNumber, previousBlockHash);
        }
        acknowledged.get().await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(
                0,
                acknowledged.get().getCount(),
                "Timed out waiting for a publish acknowledgement of block >= " + LAST_PUBLISHED_BLOCK);
        stream.closeConnection();
        publishClient.close();
    }

    private PbjGrpcClient createGrpcClient() {
        final Duration timeout = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + serverPort)
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
}
