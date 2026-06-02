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
 * End-to-end test for the state-management-hashgraph plugin. Boots {@link BlockNodeApp} in-JVM
 * with the live-state plugin on the classpath and exercises the
 * {@code StateService} gRPC endpoint over the network (via {@link PbjGrpcClient}) —
 * the same wire path a real client uses.
 *
 * <p>No blocks are streamed in this harness, so under the lag-1 commit model the plugin
 * has no network-attested state and answers every query with {@code NOT_READY}. The test
 * therefore verifies that:
 *
 * <ul>
 *   <li>the app reaches RUNNING — the live-state plugin doesn't break startup;</li>
 *   <li>the {@code StateService} endpoints ({@code getBinarySingleton}, {@code getBinaryKV},
 *       {@code getBinaryQueue}) are reachable over the wire and return a structured
 *       {@code NOT_READY} response carrying {@code state_metadata}.</li>
 * </ul>
 *
 * Each test runs the BlockNodeApp through a fresh boot/shutdown cycle with state paths
 * scoped to {@code build/tmp/state-e2e}, mirroring {@code BlockNodeCloudStorageTests}'
 * S3Mock pattern.
 */
@Tag("api")
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class StateManagementE2ETests {

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
                "state.management.stateMetadataPath",
                stateRoot.resolve("stateMetadata.json").toString());
        System.setProperty(
                "state.management.stateSnapshotRecentPath",
                stateRoot.resolve("recent").toString());

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
                app.shutdown("StateManagementE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread teardown.
            }
        }
    }

    private static void clearProperties() {
        System.clearProperty("state.management.stateMetadataPath");
        System.clearProperty("state.management.stateSnapshotRecentPath");
    }

    @Test
    void getBinarySingletonRoundTripsThroughGrpc() {
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        // No blocks streamed → no network-attested state → NOT_READY under lag-1. The point
        // is that the StateService is wired, reachable over the wire, and returns a
        // structured response carrying metadata.
        final BinaryStateQueryResponse response = client.getBinarySingleton(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(1L).build());

        assertThat(response.status()).as("structured response over the wire").isEqualTo(Code.NOT_READY);
        assertThat(response.stateMetadata()).as("metadata always populated").isNotNull();
    }

    @Test
    void getBinaryKVAndQueueReturnStructuredResponses() {
        final var client = new StateServiceInterface.StateServiceClient(grpcClient, OPTIONS);

        // No blocks streamed → NOT_READY for every query (lag-1: nothing attested yet).
        final BinaryStateQueryResponse kv = client.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(99L)
                .keyBytes(Bytes.fromHex("01"))
                .build());
        assertThat(kv.status()).isEqualTo(Code.NOT_READY);

        final BinaryStateQueryResponse queue = client.getBinaryQueue(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(99L).build());
        assertThat(queue.status()).isEqualTo(Code.NOT_READY);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

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
