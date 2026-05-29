// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
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
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.api.StateServiceInterface;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end test for the state-hashgraph-live plugin. Boots {@link BlockNodeApp} in-JVM
 * with the live-state plugin on the classpath and exercises the
 * {@code StateService} gRPC endpoint over the network (via {@link PbjGrpcClient}) —
 * the same wire path a real client uses. Verifies:
 *
 * <ul>
 *   <li>the app reaches RUNNING — the live-state plugin doesn't break startup;</li>
 *   <li>{@code getBinarySingleton} reaches the running plugin, returns a structured
 *       response code, and includes {@code state_metadata} on the wire;</li>
 *   <li>{@code getBinaryKV} on a missing key returns {@code NOT_FOUND};</li>
 *   <li>{@code getBinaryQueue} on a missing queue returns {@code NOT_FOUND}.</li>
 * </ul>
 *
 * Each test runs the BlockNodeApp through a fresh boot/shutdown cycle with state paths
 * scoped to {@code build/tmp/state-e2e}, mirroring {@code BlockNodeCloudStorageTests}'
 * S3Mock pattern.
 */
@Tag("api")
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class LiveStateE2ETests {

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient grpcClient;

    @BeforeEach
    void setUp() throws Exception {
        final Path stateRoot = Path.of("build/tmp/state-e2e").toAbsolutePath();
        if (Files.exists(stateRoot)) {
            Files.walk(stateRoot)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        Files.createDirectories(stateRoot);
        System.setProperty(
                "state.live.stateMetadataPath",
                stateRoot.resolve("stateMetadata.json").toString());
        System.setProperty(
                "state.live.stateSnapshotRecentPath",
                stateRoot.resolve("recent").toString());
        System.setProperty(
                "state.live.stateSnapshotHistoricPath",
                stateRoot.resolve("historic").toString());

        final Path dataDir = Paths.get("build/tmp/data").toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(State.RUNNING, app.blockNodeState(), "BlockNodeApp must be RUNNING after startup");

        grpcClient = createGrpcClient();
    }

    @AfterEach
    void tearDown() {
        clearProperties();
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("LiveStateE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread teardown.
            }
        }
    }

    private static void clearProperties() {
        System.clearProperty("state.live.stateMetadataPath");
        System.clearProperty("state.live.stateSnapshotRecentPath");
        System.clearProperty("state.live.stateSnapshotHistoricPath");
    }

    @Test
    void getBinarySingletonRoundTripsThroughGrpc() {
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        // Poll briefly until the plugin finishes catch-up (no historical blocks means it
        // completes near-immediately, but the catch-up thread is still asynchronous).
        final BinaryStateQueryResponse response = awaitNonNotReady(
                () -> client.getBinarySingleton(BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(1L).build()));

        assertThat(response.status()).as("structured response over the wire").isIn(Code.NOT_FOUND, Code.SUCCESS);
        assertThat(response.stateMetadata()).as("metadata always populated").isNotNull();
    }

    @Test
    void getBinaryKVAndQueueReturnStructuredResponses() {
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        // KV on a non-existent state/key.
        final BinaryStateQueryResponse kv = awaitNonNotReady(() -> client.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(99L)
                .keyBytes(Bytes.fromHex("01"))
                .build()));
        assertThat(kv.status()).isEqualTo(Code.NOT_FOUND);

        // Queue with no elements.
        final BinaryStateQueryResponse queue = awaitNonNotReady(() -> client.getBinaryQueue(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(99L).build()));
        assertThat(queue.status()).isEqualTo(Code.NOT_FOUND);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /**
     * Polls the supplied gRPC call until the plugin reports a non-{@code NOT_READY}
     * response or the timeout elapses. Returns the final response.
     */
    private static BinaryStateQueryResponse awaitNonNotReady(
            final java.util.function.Supplier<BinaryStateQueryResponse> call) {
        final long deadline = System.currentTimeMillis() + 10_000L;
        BinaryStateQueryResponse response = call.get();
        while (response.status() == Code.NOT_READY && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            response = call.get();
        }
        return response;
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
