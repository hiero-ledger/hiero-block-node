// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/// E2E tests validating the admission-control / throttling mechanism (see
/// `docs/design/apis/api-throttling.md`) against a real, fully-wired [BlockNodeApp], not mocks.
/// Each test overrides throttle config to small, easy-to-exceed values via system properties
/// before starting the app, so tests stay fast and deterministic instead of racing real rate
/// limits.
@Tag("api")
public class BlockNodeThrottleTests {
    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final ServerStatusRequest SIMPLE_SERVER_STATUS_REQUEST =
            ServerStatusRequest.newBuilder().build();
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    /// Every system property key this test set, so [#afterEach()] can clear exactly those.
    private final Set<String> overriddenPropertyKeys = new HashSet<>();

    @BeforeEach
    void beforeEach() throws IOException {
        final Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
        BlockItemBuilderUtils.provisionTssBootstrap();
    }

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BlockNodeThrottleTests", "test teardown");
        }
        overriddenPropertyKeys.forEach(System::clearProperty);
        overriddenPropertyKeys.clear();
    }

    /// Sets the given config overrides as system properties, then constructs and starts the app.
    private void startApp(final Map<String, String> throttleOverrides) throws InterruptedException, IOException {
        throttleOverrides.forEach((key, value) -> {
            System.setProperty(key, value);
            overriddenPropertyKeys.add(key);
        });
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        Thread.sleep(200); // short pause to allow async startup tasks to complete
        assertEquals(State.RUNNING, app.blockNodeState());
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

    /// Repeats `call` until it throws a `RESOURCE_EXHAUSTED` rejection, or fails after `attempts`
    /// tries. Retries since a loaded test machine can legitimately space back-to-back calls more
    /// than the ~1-second GCRA window apart, even at the tightest rate setting.
    private static void assertEventuallyRejectedByThrottle(final int attempts, final ThrowingRunnable call) {
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                call.run();
            } catch (final RuntimeException e) {
                assertThat(e.getCause()).isInstanceOf(GrpcException.class);
                assertThat(((GrpcException) e.getCause()).status()).isEqualTo(GrpcStatus.RESOURCE_EXHAUSTED);
                return;
            }
        }
        fail("Expected at least one of " + attempts + " rapid back-to-back calls to be rejected by the throttle");
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    @Test
    @DisplayName("serverStatus: a second call faster than the configured rate is rejected")
    void serverStatusRateLimitRejectsSecondCallWithinWindow() throws InterruptedException, IOException {
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.serverStatus.ratePerSecond", "1");
        overrides.put("throttle.serverStatus.burstTolerance", "0");
        overrides.put("throttle.serverStatus.maxConcurrentPerClient", "1000");
        startApp(overrides);

        final BlockNodeServiceInterface.BlockNodeServiceClient client =
                new BlockNodeServiceInterface.BlockNodeServiceClient(createGrpcClient(), OPTIONS);
        try {
            final ServerStatusResponse first = client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST);
            assertNotNull(first);

            assertEventuallyRejectedByThrottle(10, () -> client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("serverStatus: burst tolerance admits a bounded number of rapid calls before rejecting")
    void serverStatusBurstToleranceAdmitsBoundedNumberOfRapidCalls() throws InterruptedException, IOException {
        final int burstTolerance = 3;
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.serverStatus.ratePerSecond", "1");
        overrides.put("throttle.serverStatus.burstTolerance", String.valueOf(burstTolerance));
        overrides.put("throttle.serverStatus.maxConcurrentPerClient", "1000");
        startApp(overrides);

        final BlockNodeServiceInterface.BlockNodeServiceClient client =
                new BlockNodeServiceInterface.BlockNodeServiceClient(createGrpcClient(), OPTIONS);
        try {
            // burstTolerance + 1 rapid calls must all be admitted before the throttle catches up.
            for (int i = 0; i < burstTolerance + 1; i++) {
                assertNotNull(client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
            }

            assertEventuallyRejectedByThrottle(10, () -> client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
        } finally {
            client.close();
        }
    }
}
