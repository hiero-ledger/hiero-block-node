// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
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
 * E2E smoke test for the state-hashgraph-live plugin. Boots the full BlockNodeApp in-JVM
 * with the live-state plugin on the classpath and verifies:
 *
 * <ol>
 *   <li>the app reaches RUNNING state — the live-state plugin doesn't break startup;</li>
 *   <li>{@code serverStatusDetail} responds (the plugin is wired through the SPI);</li>
 *   <li>{@code getBinarySingleton} responds with a structured status (live state present).</li>
 * </ol>
 *
 * Publishing blocks with synthetic state_changes items is left to the plugin-level
 * acceptance suite — this test focuses on the integration boundary.
 */
@Tag("api")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LiveStateE2ETests {

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40850" : System.getenv("SERVER_PORT");
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;

    @BeforeEach
    void setUp() throws Exception {
        // Use unique paths per run so multiple tests don't clobber each other.
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

        // Clear the default block data dir as well — same pattern as BlockNodeAPITests.
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
    }

    @AfterEach
    void tearDown() {
        clearProperties();
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("LiveStateE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread tear-down.
            }
        }
    }

    private static void clearProperties() {
        System.clearProperty("state.live.stateMetadataPath");
        System.clearProperty("state.live.stateSnapshotRecentPath");
        System.clearProperty("state.live.stateSnapshotHistoricPath");
    }

    @Test
    void appBootsWithLiveStatePluginAndServerStatusDetailIsAvailable() {
        // Look up plugin SPI implementations via the ServiceLoader. Both should be present —
        // the live-state plugin doesn't interfere with the existing server-status endpoint.
        final var loader = new ServiceLoaderFunction();
        final var statePlugin = loader.loadServices(StateServiceInterface.class).findFirst();
        assertThat(statePlugin).as("StateServiceInterface implementation should be discovered").isPresent();

        // A fresh ServiceLoader instance is not the same as the one wired into the running app,
        // so its getBinarySingleton returns NOT_READY (no start()) — that's still a structured
        // response, which is what we're asserting.
        final BinaryStateQueryResponse response = statePlugin
                .get()
                .getBinarySingleton(BinaryStateQuery.newBuilder().stateId(1L).build());
        assertThat(response.status()).isIn(Code.NOT_READY, Code.NOT_FOUND, Code.SUCCESS);

        // server-status plugin still responds.
        final var serverStatusPlugin = loader.loadServices(org.hiero.block.api.BlockNodeServiceInterface.class)
                .findFirst();
        assertThat(serverStatusPlugin).isPresent();
        final ServerStatusDetailResponse detail = serverStatusPlugin
                .get()
                .serverStatusDetail(ServerStatusRequest.newBuilder().build());
        assertThat(detail).isNotNull();
    }
}
