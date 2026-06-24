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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.StateServiceInterface;
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

/**
 * End-to-end test for the state-management-hashgraph plugin. Boots {@link BlockNodeApp} in-JVM
 * with the live-state plugin on the classpath and exercises the {@code StateService} gRPC endpoint
 * over the network (via {@link PbjGrpcClient}) — the same wire path a real client uses.
 *
 * <p>Two paths are covered:
 *
 * <ul>
 *   <li><b>Seeded state.</b> A minimal state snapshot is generated once (see
 *       {@link StateSeedSnapshotGenerator}) and loaded by the plugin on boot, so the plugin has
 *       network-attested state. The tests then assert that {@code getBinarySingleton},
 *       {@code getBinaryKV}, and {@code getBinaryQueue} return {@code SUCCESS} with the exact seeded
 *       bytes — real applied-state reads over the actual gRPC wire.</li>
 *   <li><b>Empty state.</b> With no snapshot, the plugin has nothing attested and answers every
 *       query with {@code NOT_READY} (lag-1), confirming the readiness gate and that the
 *       {@code StateService} is reachable and returns a structured response carrying metadata.</li>
 * </ul>
 *
 * Each test boots {@link BlockNodeApp} through a fresh boot/shutdown cycle with state paths scoped
 * under {@code build/tmp}, mirroring {@code BlockNodeCloudStorageTests}' isolation pattern.
 */
@Tag("api")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class StateManagementE2ETests {

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    /** Generated once; copied into a fresh run directory per seeded test. */
    private static final Path SEED_DIR = Path.of("build/tmp/state-e2e-seed").toAbsolutePath();

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient grpcClient;

    @BeforeAll
    static void generateSeedSnapshot() throws Exception {
        deleteRecursively(SEED_DIR);
        Files.createDirectories(SEED_DIR);
        StateSeedSnapshotGenerator.generate(SEED_DIR);
    }

    @AfterEach
    void tearDown() {
        clearProperties();
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("StateManagementE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread teardown.
            }
        }
    }

    @Test
    void seededSingletonReadReturnsSuccessOverGrpc() throws Exception {
        bootAppWithSeededState();
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        final BinaryStateQueryResponse response = client.getBinarySingleton(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                .build());

        assertThat(response.status()).as("seeded singleton must be served").isEqualTo(Code.SUCCESS);
        assertThat(response.singletonBytes())
                .as("singleton bytes round-trip the seeded value")
                .isEqualTo(StateSeedSnapshotGenerator.SINGLETON_VALUE_ENCODED);
        assertThat(response.stateMetadata()).as("metadata populated").isNotNull();
    }

    @Test
    void seededKvAndQueueReadsReturnSuccessOverGrpc() throws Exception {
        bootAppWithSeededState();
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        final BinaryStateQueryResponse kv = client.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.KV_STATE_ID)
                .keyBytes(StateSeedSnapshotGenerator.KV_KEY_ENCODED)
                .build());
        assertThat(kv.status()).as("seeded KV must be served").isEqualTo(Code.SUCCESS);
        assertThat(kv.kvBytes())
                .as("KV bytes round-trip the seeded value")
                .isEqualTo(StateSeedSnapshotGenerator.KV_VALUE_ENCODED);

        final BinaryStateQueryResponse queue = client.getBinaryQueue(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.QUEUE_STATE_ID)
                .build());
        assertThat(queue.status()).as("seeded queue must be served").isEqualTo(Code.SUCCESS);
        assertThat(queue.queueBytes()).as("queue carries the seeded element").isNotEmpty();
    }

    @Test
    void seededStateAppliesStreamedBlocksAndProgressesOverGrpc() throws Exception {
        // Generate a seed (block 0) plus the footer-hash chain for blocks 1 and 2, then boot the app
        // on a fresh copy so the plugin loads block 0 as attested.
        final Path chainSeedDir = Path.of("build/tmp/state-e2e-chain-seed").toAbsolutePath();
        deleteRecursively(chainSeedDir);
        Files.createDirectories(chainSeedDir);
        final SeededChain chain = StateSeedSnapshotGenerator.generateWithChain(chainSeedDir);

        final Path runDir = freshRunDir();
        copyRecursively(chainSeedDir, runDir);
        setStateProperties(runDir);
        bootApp();

        final var stateClient = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        // Pre-stream: only the seed (block 0) is exposed.
        final BinaryStateQueryResponse preSingleton = stateClient.getBinarySingleton(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                .build());
        assertThat(preSingleton.status()).isEqualTo(Code.SUCCESS);
        assertThat(preSingleton.singletonBytes()).isEqualTo(StateSeedSnapshotGenerator.SINGLETON_VALUE_ENCODED);
        assertThat(preSingleton.stateMetadata().blockNumber())
                .as("only the seed block is exposed before streaming")
                .isEqualTo(0L);

        // Stream a throwaway block 0 (so the BN head is 0 and the publisher accepts a contiguous chain),
        // then verifiable block 1 (carrying the changes) and block 2 (which attests block 1 under lag-1).
        final var publishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishClient.publishBlockStream(observer);

        final Bytes block0Hash = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes block1Hash = BlockItemBuilderUtils.computeBlockHashWithState(
                1L, block0Hash, chain.block1FooterHash(), chain.block1StateChanges());

        streamBlock(stream, observer, BlockItemBuilderUtils.createSimpleBlockWithNumber(0L, null), 0L);
        streamBlock(
                stream,
                observer,
                BlockItemBuilderUtils.createVerifiableBlockWithState(
                        1L, block0Hash, chain.block1FooterHash(), chain.block1StateChanges()),
                1L);
        streamBlock(
                stream,
                observer,
                BlockItemBuilderUtils.createVerifiableBlockWithState(2L, block1Hash, chain.block2FooterHash(), null),
                2L);

        // The plugin applies asynchronously; wait until block 1 is exposed (progression 0 → 1).
        awaitExposedBlock(stateClient, 1L);

        // Block progression confirmed, and all three shapes reflect the block-1 changes.
        final BinaryStateQueryResponse singleton = stateClient.getBinarySingleton(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                .build());
        assertThat(singleton.status()).isEqualTo(Code.SUCCESS);
        assertThat(singleton.stateMetadata().blockNumber())
                .as("exposed block progressed to 1")
                .isEqualTo(1L);
        assertThat(singleton.singletonBytes())
                .as("singleton reflects the block-1 update")
                .isEqualTo(StateSeedSnapshotGenerator.BLOCK1_SINGLETON_VALUE_ENCODED);

        final BinaryStateQueryResponse kv = stateClient.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.KV_STATE_ID)
                .keyBytes(StateSeedSnapshotGenerator.BLOCK1_KV_KEY_ENCODED)
                .build());
        assertThat(kv.status()).as("block-1 KV key is now present").isEqualTo(Code.SUCCESS);
        assertThat(kv.kvBytes()).isEqualTo(StateSeedSnapshotGenerator.BLOCK1_KV_VALUE_ENCODED);

        final BinaryStateQueryResponse queue = stateClient.getBinaryQueue(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(StateSeedSnapshotGenerator.QUEUE_STATE_ID)
                .build());
        assertThat(queue.status()).isEqualTo(Code.SUCCESS);
        assertThat(queue.queueBytes())
                .as("queue holds the seed element plus the block-1 element")
                .containsExactly(
                        StateSeedSnapshotGenerator.QUEUE_ELEMENT_ENCODED,
                        StateSeedSnapshotGenerator.BLOCK1_QUEUE_ELEMENT_ENCODED);

        publishClient.close();
    }

    @Test
    void emptyStateReturnsNotReadyOverGrpc() throws Exception {
        bootAppWithEmptyState();
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        // No snapshot loaded → nothing attested → NOT_READY under lag-1, but the StateService is
        // still wired, reachable over the wire, and returns a structured response with metadata.
        final BinaryStateQueryResponse response = client.getBinarySingleton(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(1L).build());
        assertThat(response.status()).isEqualTo(Code.NOT_READY);
        assertThat(response.stateMetadata()).as("metadata always populated").isNotNull();
    }

    // ── Boot helpers ─────────────────────────────────────────────────────────

    /** Boot the app with a fresh copy of the generated seed snapshot loaded by the plugin. */
    private void bootAppWithSeededState() throws Exception {
        final Path runDir = freshRunDir();
        copyRecursively(SEED_DIR, runDir);
        setStateProperties(runDir);
        bootApp();
    }

    /** Boot the app with no snapshot, so the plugin starts empty (NOT_READY). */
    private void bootAppWithEmptyState() throws Exception {
        final Path runDir = freshRunDir();
        Files.createDirectories(runDir.resolve("recent"));
        setStateProperties(runDir);
        bootApp();
    }

    private static Path freshRunDir() throws Exception {
        final Path runDir = Path.of("build/tmp/state-e2e-run").toAbsolutePath();
        deleteRecursively(runDir);
        Files.createDirectories(runDir);
        // Block storage must be empty so historical catch-up does not look past the seed block.
        deleteRecursively(Paths.get("build/tmp/data").toAbsolutePath());
        return runDir;
    }

    private static void setStateProperties(final Path runDir) {
        System.setProperty(
                "state.management.stateMetadataPath",
                runDir.resolve("stateMetadata.json").toString());
        System.setProperty(
                "state.management.stateSnapshotRecentPath",
                runDir.resolve("recent").toString());
    }

    private void bootApp() throws Exception {
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(State.RUNNING, app.blockNodeState(), "BlockNodeApp must be RUNNING after startup");
        grpcClient = createGrpcClient();
    }

    /** Send a block's items plus its end-of-block marker, then await an ACK for {@code blockNumber}. */
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

    /** Poll the state service until the exposed block reaches {@code expectedBlock} (or time out). */
    private static void awaitExposedBlock(
            final StateServiceInterface.StateServiceClient client, final long expectedBlock)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            final BinaryStateQueryResponse response = client.getBinarySingleton(BinaryStateQuery.newBuilder()
                    .retrieveLatest(true)
                    .stateId(StateSeedSnapshotGenerator.SINGLETON_STATE_ID)
                    .build());
            if (response.status() == Code.SUCCESS
                    && response.stateMetadata() != null
                    && response.stateMetadata().blockNumber() >= expectedBlock) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private static void clearProperties() {
        System.clearProperty("state.management.stateMetadataPath");
        System.clearProperty("state.management.stateSnapshotRecentPath");
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
                    Files.createDirectories(dest.getParent());
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
