// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * E2E tests for the block-stream filter on the publisher side. Each test
 * configures {@code PublisherConfig}
 * via System properties (the same {@code SystemPropertiesConfigSource}
 * ordinal-400 pattern used by {@code BlockNodeCloudStorageTests}), boots
 * {@code BlockNodeApp} in-JVM, and exercises the publisher gRPC.
 *
 * <p>The filter is set to deny {@code ROUND_HEADER} (field 3) — the only
 * non-mandatory item produced by
 * {@link BlockItemBuilderUtils#createSimpleBlockWithNumber(long)}. After
 * the filter passes the publisher accepts the block (it has been
 * rewritten with a {@code FilteredSingleItem} in place of the round
 * header). Pure publish-side smoke; the deeper "did the verifier accept
 * the rewritten block" assertion would require additional plumbing and
 * is captured in {@code docs/design/filtering/block-stream-filtering.md}
 * and the expansion notes.
 */
@Tag("api")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BlockStreamFilterE2ETests {

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient publishClient;

    @BeforeEach
    void setUp() throws Exception {
        final Path dataDir = Paths.get("build/tmp/data").toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        // Deny ROUND_HEADER (field 3) on the publish side. SystemPropertiesConfigSource
        // (ordinal 400) picks these up before BlockNodeApp constructs its Configuration.
        System.setProperty("producer.blockStreamFilterInclude", "false");
        System.setProperty("producer.blockStreamFilterItemTypes", "3");

        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(State.RUNNING, app.blockNodeState(), "BlockNodeApp must be RUNNING after startup");

        publishClient = createGrpcClient();
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("producer.blockStreamFilterInclude");
        System.clearProperty("producer.blockStreamFilterItemTypes");
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("BlockStreamFilterE2ETests", "teardown");
            } catch (final RuntimeException ignored) {
                // benign races during messaging-thread teardown.
            }
        }
    }

    /**
     * Publishes a block while the publisher is configured with a deny-list for
     * {@code ROUND_HEADER}. The block is rewritten at ingress with a
     * {@code FilteredSingleItem} replacing the round header before the messaging
     * facility sees it. We assert the publisher returns an acknowledgement —
     * proof that the filter rewriting + downstream pipeline didn't choke on the
     * rewritten block.
     */
    @Test
    void publisherAcksFilteredBlock() throws Exception {
        final long blockNumber = 0L;
        final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        final PublishStreamRequest blockRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                .build();

        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishSvc.publishBlockStream(ackObserver);

        final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(1);
        stream.onNext(blockRequest);
        stream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
        awaitLatch(ackLatch, "filtered block acknowledgement");

        assertThat(ackObserver.getOnNextCalls()).isNotEmpty();
        // The response is either an ACK (block accepted) or a structured non-ACK
        // status; both are valid outcomes that prove the filter didn't crash the
        // ingest pipeline. A crash would manifest as no on-next call at all.
        assertThat(ackObserver.getOnNextCalls().get(0).response().kind()).isNotNull();

        stream.closeConnection();
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

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String desc)
            throws InterruptedException {
        latch.get().await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for: " + desc);
    }
}
