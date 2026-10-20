// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.protoc.BlockStreamSubscribeServiceGrpc;
import org.hiero.block.api.protoc.SubscribeStreamRequest;
import org.hiero.block.api.protoc.SubscribeStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.metrics.MetricsAccessor;
import org.hiero.block.suites.metrics.MetricsAccessor.MetricType;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * E2E tests for subscriber sessions whose client disconnected while the session waited for blocks.
 * <p>
 * PBJ does not propagate a client cancel to the subscriber session, so a session that sends
 * nothing never learns its client is gone. These tests cancel real gRPC streams on a real
 * BlockNodeApp and verify the idle session limit releases those sessions, as reported by the
 * {@code subscriber_open_connections} gauge.
 */
@Tag("api")
public class SubscriberIdleSessionApiTests {
    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final String IDLE_TIMEOUT_PROPERTY = "subscriber.idleSessionTimeoutSeconds";
    private static final String IDLE_TIMEOUT_SECONDS = "2";
    private static final String REAP_INTERVAL_PROPERTY = "subscriber.completedSessionReapIntervalSeconds";
    private static final String REAP_INTERVAL_SECONDS = "1";
    /** The suites' app-test.properties disables the metrics endpoint; these tests read the gauge from it. */
    private static final String METRICS_ENABLED_PROPERTY = "metrics.exporter.openmetrics.http.enabled";

    private static final String METRICS_PORT_PROPERTY = "metrics.exporter.openmetrics.http.port";
    private static final String METRICS_PORT = "16007";
    private static final String OPEN_CONNECTIONS_METRIC = "subscriber_open_connections";
    private static final String METRICS_ENDPOINT = "http://localhost:" + METRICS_PORT + "/metrics";
    private static final int SUBSCRIBER_COUNT = 20;
    /** Covers the idle limit plus the reaper interval, with margin. */
    private static final Duration RELEASE_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final long UNBOUNDED = -1L;
    private static final PbjOptions PBJ_OPTIONS =
            new PbjOptions(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private final MetricsAccessor metricsAccessor = new MetricsAccessor(METRICS_ENDPOINT);

    private BlockNodeApp app;
    private ManagedChannel channel;

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
        System.setProperty(REAP_INTERVAL_PROPERTY, REAP_INTERVAL_SECONDS);
        System.setProperty(METRICS_ENABLED_PROPERTY, "true");
        System.setProperty(METRICS_PORT_PROPERTY, METRICS_PORT);
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        assertThat(app.blockNodeState()).isEqualTo(State.RUNNING);
        channel = ManagedChannelBuilder.forAddress("localhost", Integer.parseInt(serverPort))
                .usePlaintext()
                .build();
    }

    @AfterEach
    void afterEach() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("SubscriberIdleSessionApiTests", "API test teardown");
        }
        System.clearProperty(IDLE_TIMEOUT_PROPERTY);
        System.clearProperty(REAP_INTERVAL_PROPERTY);
        System.clearProperty(METRICS_ENABLED_PROPERTY);
        System.clearProperty(METRICS_PORT_PROPERTY);
    }

    @Test
    void cancelledLiveTipSubscribersAreReleased() throws IOException, InterruptedException {
        cancel(subscribeAll(SUBSCRIBER_COUNT, UNBOUNDED, UNBOUNDED));

        assertThat(awaitOpenConnections(0)).isZero();
    }

    @Test
    void cancelledCaughtUpSubscribersAreReleased() throws IOException, InterruptedException, NoSuchAlgorithmException {
        publishBlockZero();

        cancel(subscribeAll(SUBSCRIBER_COUNT, 0L, UNBOUNDED));

        assertThat(awaitOpenConnections(0)).isZero();
    }

    /** Opens {@code count} subscriptions on one connection and waits until the node tracks all of them. */
    private List<CancellableObserver> subscribeAll(final int count, final long startBlock, final long endBlock)
            throws IOException, InterruptedException {
        final List<CancellableObserver> subscriptions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            subscriptions.add(subscribe(startBlock, endBlock));
        }
        assertThat(awaitOpenConnections(count)).isEqualTo(count);
        return subscriptions;
    }

    /** Cancels the subscriptions from the client side, as a client that disconnects would. */
    private void cancel(final List<CancellableObserver> subscriptions) {
        for (final CancellableObserver subscription : subscriptions) {
            subscription.call.cancel("client disconnect", null);
        }
    }

    private CancellableObserver subscribe(final long startBlock, final long endBlock) {
        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .setStartBlockNumber(startBlock)
                .setEndBlockNumber(endBlock)
                .build();
        final CancellableObserver observer = new CancellableObserver();
        BlockStreamSubscribeServiceGrpc.newStub(channel).subscribeBlockStream(request, observer);
        return observer;
    }

    /**
     * Polls the open connections gauge until it equals the expected value or the release timeout
     * passes, and returns the last value read.
     */
    private long awaitOpenConnections(final long expected) throws IOException, InterruptedException {
        final long deadlineNanos = System.nanoTime() + RELEASE_TIMEOUT.toNanos();
        long openConnections = metricsAccessor.getMetricValue(OPEN_CONNECTIONS_METRIC, MetricType.GAUGE);
        while (openConnections != expected && System.nanoTime() < deadlineNanos) {
            LockSupport.parkNanos(POLL_INTERVAL.toNanos());
            openConnections = metricsAccessor.getMetricValue(OPEN_CONNECTIONS_METRIC, MetricType.GAUGE);
        }
        return openConnections;
    }

    private void publishBlockZero() throws InterruptedException, NoSuchAlgorithmException {
        final PbjGrpcClient publishClient = createPbjGrpcClient();
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishServiceClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, PBJ_OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> responseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> requestStream =
                publishServiceClient.publishBlockStream(responseObserver);
        final BlockItem[] blockItems = BlockItemBuilderUtils.createSimpleBlockWithNumber(0L);
        final AtomicReference<CountDownLatch> acknowledged = responseObserver.setAndGetOnNextLatch(1);
        requestStream.onNext(PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems).build())
                .build());
        requestStream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(0L).build())
                .build());
        assertThat(acknowledged.get().await(RELEASE_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                .isTrue();
    }

    private PbjGrpcClient createPbjGrpcClient() {
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
                webClient, new PbjGrpcClientConfig(timeout, tls, PBJ_OPTIONS.authority(), PBJ_OPTIONS.contentType()));
    }

    private record PbjOptions(Optional<String> authority, String contentType)
            implements ServiceInterface.RequestOptions {}

    /**
     * Exposes the client call so the test can cancel it; responses are ignored. Cancelled streams on
     * one connection also show helidon-io/helidon#12771: a cancel or reset of an older stream after a
     * newer one was opened makes the server close the whole connection, so these tests only check
     * that sessions are released, not that other streams on the connection survive.
     */
    private static final class CancellableObserver
            implements ClientResponseObserver<SubscribeStreamRequest, SubscribeStreamResponse> {
        private ClientCallStreamObserver<SubscribeStreamRequest> call;

        @Override
        public void beforeStart(final ClientCallStreamObserver<SubscribeStreamRequest> requestStream) {
            call = requestStream;
        }

        @Override
        public void onNext(final SubscribeStreamResponse response) {
            // Only session release is checked, through the open connections gauge.
        }

        @Override
        public void onError(final Throwable error) {
            // Expected for cancelled calls.
        }

        @Override
        public void onCompleted() {
            // Nothing to record.
        }
    }
}
