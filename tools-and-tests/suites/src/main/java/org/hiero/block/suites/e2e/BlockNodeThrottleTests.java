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

/**
 * E2E tests validating the admission-control / throttling mechanism (see
 * {@code docs/design/apis/api-throttling.md}) actually rejects and admits calls against a real,
 * fully-wired {@link BlockNodeApp} — not mocks. Each test overrides the relevant throttle
 * configuration to small, easy-to-exceed values via system properties (the same override mechanism
 * every plugin's config already supports) before starting the app, so tests can be short and
 * deterministic instead of racing real-world rate limits.
 */
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
    /** Every system property key this test set, so {@link #afterEach()} can clear exactly those. */
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

    /** Sets the given config overrides as system properties, then constructs and starts the app. */
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

    /**
     * Repeats {@code call} until it throws a {@code RESOURCE_EXHAUSTED} rejection, or fails after
     * {@code attempts} tries. A GCRA rate limit configured at its tightest possible setting
     * (1/second, no burst) still only guarantees rejection of a call arriving within the same
     * ~1-second window as the previous one — a single pair of back-to-back calls can, on a loaded
     * test machine, legitimately land more than a second apart. Repeating the attempt makes the
     * assertion robust to that scheduling jitter without weakening what it actually proves: the
     * throttle does reject calls, it isn't just always admitting everything.
     */
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
            // burstTolerance pacing intervals of slack means burstTolerance + 1 rapid calls must
            // all be admitted before the throttle catches up.
            for (int i = 0; i < burstTolerance + 1; i++) {
                assertNotNull(client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
            }

            // The burst allowance is now exhausted; further rapid calls must eventually be rejected.
            assertEventuallyRejectedByThrottle(10, () -> client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
        } finally {
            client.close();
        }
    }
}
