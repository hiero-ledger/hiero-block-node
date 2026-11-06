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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.QueueRange;
import org.hiero.block.api.StateQuery;
import org.hiero.block.api.StateResponse;
import org.hiero.block.api.StateResponse.Code;
import org.hiero.block.api.StateServiceInterface.StateServiceClient;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.hiero.block.suites.utils.StateSeedSnapshotGenerator;
import org.hiero.block.suites.utils.StateSeedSnapshotGenerator.SeededChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// `StateService` over the real gRPC wire against an in-JVM `BlockNodeApp` with the
/// `state-management-hashgraph` plugin on the module path. Seeded tests restore a snapshot from
/// [StateSeedSnapshotGenerator]; the empty-state test boots with none. Each test boots a fresh
/// app with state under `build/tmp`, like `BlockNodeCloudStorageTests`.
@Tag("api")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class StateManagementE2ETests {

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);
    private static final String RECENT_PATH_PROPERTY = "state.management.snapshotRecentPath";
    private static final long SEED = StateSeedSnapshotGenerator.SEED_BLOCK_NUMBER;

    /// Generated once; the snapshot directory is copied into a fresh run directory per test.
    private static final Path SEED_RECENT =
            Path.of("build/tmp/state-e2e-seed/recent").toAbsolutePath();

    private static final Path RUN_RECENT =
            Path.of("build/tmp/state-e2e-run/recent").toAbsolutePath();

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private static SeededChain chain;

    private BlockNodeApp app;
    private PbjGrpcClient grpcClient;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @BeforeAll
    static void generateSeedSnapshot() throws Exception {
        deleteRecursively(SEED_RECENT.getParent());
        chain = StateSeedSnapshotGenerator.generate(SEED_RECENT);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (final AutoCloseable closeable : closeables) {
            closeable.close();
        }
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("StateManagementE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread teardown
            }
        }
        System.clearProperty(RECENT_PATH_PROPERTY);
    }

    @Test
    void seededStateIsServedOverGrpc() throws Exception {
        final StateServiceClient client = bootApp(true);

        final StateResponse singleton = client.getSingleton(
                latest(StateSeedSnapshotGenerator.SINGLETON_STATE_ID).build());
        assertThat(singleton.status()).isEqualTo(Code.SUCCESS);
        assertThat(singleton.blockNumber()).isEqualTo(SEED);
        assertThat(singleton.singletonBytes()).isEqualTo(StateSeedSnapshotGenerator.SINGLETON_VALUE);

        final StateResponse kv = client.getKV(latest(StateSeedSnapshotGenerator.KV_STATE_ID)
                .keyBytes(StateSeedSnapshotGenerator.KV_KEY)
                .build());
        assertThat(kv.status()).isEqualTo(Code.SUCCESS);
        assertThat(kv.blockNumber()).isEqualTo(SEED);
        assertThat(kv.kvBytes()).isEqualTo(StateSeedSnapshotGenerator.KV_VALUE);

        final StateResponse queue = client.getQueue(StateQuery.newBuilder()
                .blockNumber(SEED)
                .stateId(StateSeedSnapshotGenerator.QUEUE_STATE_ID)
                .build());
        assertThat(queue.status()).isEqualTo(Code.SUCCESS);
        assertThat(queue.blockNumber()).isEqualTo(SEED);
        assertThat(queue.queueValuesOrThrow().values())
                .containsExactlyElementsOf(StateSeedSnapshotGenerator.QUEUE_ELEMENTS);

        final StateResponse tail = client.getQueue(latest(StateSeedSnapshotGenerator.QUEUE_STATE_ID)
                .queueRange(new QueueRange(2L, 2L))
                .build());
        assertThat(tail.status()).isEqualTo(Code.SUCCESS);
        assertThat(tail.queueValuesOrThrow().values())
                .containsExactly(StateSeedSnapshotGenerator.QUEUE_ELEMENTS.get(1));
    }

    @Test
    void nonAttestedBlockNumberIsNotAvailableOverGrpc() throws Exception {
        final StateServiceClient client = bootApp(true);

        final StateResponse response = client.getSingleton(StateQuery.newBuilder()
                .blockNumber(SEED + 1)
                .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                .build());

        assertThat(response.status()).isEqualTo(Code.NOT_AVAILABLE);
        assertThat(response.blockNumber()).isEqualTo(SEED);
        assertThat(response.hasSingletonBytes()).isFalse();
    }

    @Test
    void emptyStateIsNotReadyOverGrpc() throws Exception {
        final StateServiceClient client = bootApp(false);

        final List<StateResponse> responses = List.of(
                client.getKV(latest(StateSeedSnapshotGenerator.KV_STATE_ID)
                        .keyBytes(StateSeedSnapshotGenerator.KV_KEY)
                        .build()),
                client.getSingleton(
                        latest(StateSeedSnapshotGenerator.SINGLETON_STATE_ID).build()),
                client.getQueue(
                        latest(StateSeedSnapshotGenerator.QUEUE_STATE_ID).build()));

        assertThat(responses).allSatisfy(response -> {
            assertThat(response.status()).isEqualTo(Code.NOT_READY);
            // uint64 max on the wire; -1 as a Java long.
            assertThat(response.blockNumber()).isEqualTo(-1L);
        });
    }

    @Test
    void streamedBlocksAdvanceTheServedStateOverGrpc() throws Exception {
        final StateServiceClient client = bootApp(true);
        final var publishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        closeables.add(publishClient::close);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishClient.publishBlockStream(observer);

        // Block 0 is already the restored state, so the plugin skips it; block 1 carries the changes
        // and block 2 attests block 1 (lag-1).
        final Bytes block0Hash = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes block1Hash = BlockItemBuilderUtils.computeBlockHashWithState(
                1L, block0Hash, chain.block1StartHash(), chain.block1StateChanges());
        streamBlock(stream, observer, BlockItemBuilderUtils.createSimpleBlockWithNumber(0L, null), 0L);
        streamBlock(
                stream,
                observer,
                BlockItemBuilderUtils.createVerifiableBlockWithState(
                        1L, block0Hash, chain.block1StartHash(), chain.block1StateChanges()),
                1L);
        streamBlock(
                stream,
                observer,
                BlockItemBuilderUtils.createVerifiableBlockWithState(2L, block1Hash, chain.block2StartHash(), null),
                2L);

        final StateResponse singleton = awaitSingletonAtBlock(client, 1L);
        assertThat(singleton.singletonBytes()).isEqualTo(StateSeedSnapshotGenerator.BLOCK1_SINGLETON_STORED);

        final StateResponse kv = client.getKV(latest(StateSeedSnapshotGenerator.KV_STATE_ID)
                .keyBytes(StateSeedSnapshotGenerator.BLOCK1_KV_KEY_STORED)
                .build());
        assertThat(kv.status()).isEqualTo(Code.SUCCESS);
        assertThat(kv.blockNumber()).isEqualTo(1L);
        assertThat(kv.kvBytes()).isEqualTo(StateSeedSnapshotGenerator.BLOCK1_KV_VALUE_STORED);

        final StateResponse queue = client.getQueue(
                latest(StateSeedSnapshotGenerator.QUEUE_STATE_ID).build());
        assertThat(queue.status()).isEqualTo(Code.SUCCESS);
        final List<Bytes> expected = new ArrayList<>(StateSeedSnapshotGenerator.QUEUE_ELEMENTS);
        expected.add(StateSeedSnapshotGenerator.BLOCK1_QUEUE_ELEMENT_STORED);
        assertThat(queue.queueValuesOrThrow().values()).containsExactlyElementsOf(expected);

        // The seed block is no longer attested.
        assertThat(client.getSingleton(StateQuery.newBuilder()
                                .blockNumber(SEED)
                                .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                                .build())
                        .status())
                .isEqualTo(Code.NOT_AVAILABLE);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// Boot a fresh app, with the seed snapshot restored when `seeded`, and return a state client.
    private StateServiceClient bootApp(final boolean seeded) throws Exception {
        deleteRecursively(RUN_RECENT.getParent());
        // Block storage must be empty so catch-up doesn't look past the seed block.
        deleteRecursively(Path.of("build/tmp/data").toAbsolutePath());
        Files.createDirectories(RUN_RECENT);
        if (seeded) {
            final String seedDir = Long.toString(SEED);
            copyRecursively(SEED_RECENT.resolve(seedDir), RUN_RECENT.resolve(seedDir));
        }
        System.setProperty(RECENT_PATH_PROPERTY, RUN_RECENT.toString());

        BlockItemBuilderUtils.provisionTssBootstrap();
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(State.RUNNING, app.blockNodeState(), "BlockNodeApp must be RUNNING after startup");
        grpcClient = createGrpcClient();
        final StateServiceClient client = new StateServiceClient(grpcClient, OPTIONS);
        closeables.add(client::close);
        return client;
    }

    private static StateQuery.Builder latest(final long stateId) {
        return StateQuery.newBuilder().retrieveLatest(true).stateId(stateId);
    }

    /// Send a block and its end-of-block marker, then wait for an acknowledgement of it.
    private static void streamBlock(
            final Pipeline<? super PublishStreamRequest> stream,
            final ResponsePipelineUtils<PublishStreamResponse> observer,
            final BlockItem[] items,
            final long blockNumber)
            throws InterruptedException {
        final AtomicReference<CountDownLatch> ackLatch = observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && Objects.requireNonNull(response.acknowledgement()).blockNumber() >= blockNumber);
        stream.onNext(PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                .build());
        stream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
        ackLatch.get().await(60, TimeUnit.SECONDS);
        assertEquals(0, ackLatch.get().getCount(), "Timed out waiting for ACK >= " + blockNumber);
    }

    /// Poll until the attested block reaches `blockNumber`; the plugin applies asynchronously.
    private static StateResponse awaitSingletonAtBlock(final StateServiceClient client, final long blockNumber)
            throws InterruptedException {
        final StateQuery query = StateQuery.newBuilder()
                .blockNumber(blockNumber)
                .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                .build();
        final long deadline = System.currentTimeMillis() + 30_000L;
        StateResponse response = client.getSingleton(query);
        while (response.status() != Code.SUCCESS && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            response = client.getSingleton(query);
        }
        assertThat(response.status())
                .as("block %d attested; last response %s", blockNumber, response)
                .isEqualTo(Code.SUCCESS);
        return response;
    }

    private static void deleteRecursively(final Path root) throws Exception {
        if (Files.exists(root)) {
            try (var walk = Files.walk(root)) {
                walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }

    private static void copyRecursively(final Path source, final Path target) throws Exception {
        try (var walk = Files.walk(source)) {
            for (final Path src : (Iterable<Path>) walk::iterator) {
                final Path dest = target.resolve(source.relativize(src).toString());
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dest);
                } else {
                    Files.copy(src, dest);
                }
            }
        }
    }

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
}
