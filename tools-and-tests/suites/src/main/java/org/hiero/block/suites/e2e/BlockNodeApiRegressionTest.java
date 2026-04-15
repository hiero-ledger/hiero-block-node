// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.IOException;
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
import java.util.function.Function;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * E2E regression tests for production bugs that required full-stack exercise to reproduce.
 * Each test documents the original failure mode in its javadoc and asserts the fixed behaviour.
 */
@Tag("api")
@DisplayName("BlockNodeApp Regression Tests")
public class BlockNodeApiRegressionTest {

    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

    private final Function<PublishStreamResponse, PublishStreamResponse.ResponseOneOfType> responseKindExtractor =
            response -> response.response().kind();
    private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
            response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();
    private final Function<PublishStreamResponse, Long> resendBlockNumberExtractor =
            response -> Objects.requireNonNull(response.resendBlock()).blockNumber();

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    private PbjGrpcClient publishBlockStreamPbjGrpcClient;
    private PbjGrpcClient getBlockPbjGrpcClient;

    @BeforeEach
    void beforeEach() throws IOException, InterruptedException {
        final Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        try {
            assertNotNull(app, "BlockNodeApp should be constructed");
            app.start();
            Thread.sleep(200);
            assertEquals(State.RUNNING, app.blockNodeState());
            publishBlockStreamPbjGrpcClient = createGrpcClient();
            getBlockPbjGrpcClient = createGrpcClient();
        } catch (final Exception e) {
            if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
                app.shutdown("BlockNodeApiRegressionTest", "test-setup-failure");
            }
            throw e;
        }
    }

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BlockNodeApiRegressionTest", "test teardown");
        }
    }

    /**
     * Reproduces the missing-block gap caused by a publisher severing its connection mid-block.
     *
     * <p>Publisher 1 streams blocks 0–2 (fully ACKed), then sends only the header for block 3
     * and severs the connection. This adds block 3 to {@code blocksToResend} via
     * {@code blockIsEnding(3)}.
     *
     * <p>Publisher 2 then streams block 4. {@code endOfBlock(4)} returns {@code RESEND(3)}
     * because {@code blocksToResend} contains 3. Publisher 2 responds by providing the full
     * block 3, filling the gap. Block 5 follows normally.
     *
     * <p>All three blocks must end up in storage. Without the fix, block 3's queue is never
     * forwarded to messaging and the gap persists silently.
     */
    @Test
    @DisplayName("publisher RESEND fills block gap left by severed connection")
    void missingBlockGapWhenPublisherSeversConnectionBeforeEndOfBlock() throws InterruptedException {
        final Bytes hash0 = BlockItemBuilderUtils.computeBlockHash(0L, null);
        final Bytes hash1 = BlockItemBuilderUtils.computeBlockHash(1L, hash0);
        final Bytes hash2 = BlockItemBuilderUtils.computeBlockHash(2L, hash1);
        final Bytes hash3 = BlockItemBuilderUtils.computeBlockHash(3L, hash2);
        final Bytes hash4 = BlockItemBuilderUtils.computeBlockHash(4L, hash3);

        // Publisher 1 — stream blocks 0–2 with proper hash chain, ACK for each.
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher1Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(
                        publishBlockStreamPbjGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher1Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher1Stream =
                publisher1Client.publishBlockStream(publisher1Observer);

        final Bytes[] previousHashes = {null, hash0, hash1};
        for (long blockNum = 0L; blockNum <= 2L; blockNum++) {
            final BlockItem[] items =
                    BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNum, previousHashes[(int) blockNum]);
            final AtomicReference<CountDownLatch> ackLatch = publisher1Observer.setAndGetOnNextLatch(1);
            publisher1Stream.onNext(buildPublishRequest(items));
            endBlock(blockNum, publisher1Stream);
            awaitLatch(ackLatch, "ACK for block " + blockNum);
        }

        // Publisher 1 sends only the header for block 3 (no proof), then severs via RESET.
        // blockIsEnding(3) removes block 3's queue from queueByBlockMap and adds 3 to
        // blocksToResend. No partial proof is stranded in blockProofs because no proof was sent.
        publisher1Stream.onNext(buildPublishRequest(new BlockItem[] {BlockItemBuilderUtils.sampleBlockHeader(3L)}));
        final AtomicReference<CountDownLatch> publisher1DisconnectLatch =
                publisher1Observer.setAndGetConnectionEndedLatch(1);
        publisher1Stream.onNext(PublishStreamRequest.newBuilder()
                .endStream(PublishStreamRequest.EndStream.newBuilder()
                        .endCode(PublishStreamRequest.EndStream.Code.RESET)
                        .build())
                .build());
        awaitLatch(publisher1DisconnectLatch, "publisher 1 disconnect after block 3 header");

        // Publisher 2 — connect and send block 4, then block 3 (the gap), then block 5.
        //
        // Sequence on the server:
        //   endOfBlock(4) -> RESEND(3): handler resets, block 4 stays in queueByBlockMap
        //   block 3 header -> getActionForHeader(3) -> ACCEPT (removes 3 from blocksToResend)
        //   endOfBlock(3) -> ACCEPT(3)
        //   endOfBlock(5) -> ACCEPT(5)
        //
        // Forwarder persistence order: block 4 first, then block 3, then block 5.
        // ACK(4) and ACK(5) are sent. Block 3 is persisted but implicit in ACK(4) (4 > 3).
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publisher2Client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> publisher2Observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> publisher2Stream =
                publisher2Client.publishBlockStream(publisher2Observer);

        // Expect 3 responses: RESEND(3), ACK(4), ACK(5).
        final AtomicReference<CountDownLatch> publisher2Latch = publisher2Observer.setAndGetOnNextLatch(3);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(4L, hash3)));
        endBlock(4L, publisher2Stream);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(3L, hash2)));
        endBlock(3L, publisher2Stream);

        publisher2Stream.onNext(buildPublishRequest(BlockItemBuilderUtils.createSimpleBlockWithNumber(5L, hash4)));
        endBlock(5L, publisher2Stream);

        awaitLatch(publisher2Latch, "RESEND(3) and ACKs for blocks 4 and 5");

        final List<PublishStreamResponse> publisher2Responses = publisher2Observer.getOnNextCalls();
        assertThat(publisher2Responses)
                .as("publisher 2 must receive RESEND(3) — gap was detected, publisher asked to fill it")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(3L, resendBlockNumberExtractor));
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block 4")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(4L, acknowledgementBlockNumberExtractor));
        assertThat(publisher2Responses)
                .as("publisher 2 must receive ACK for block 5")
                .anySatisfy(response -> assertThat(response)
                        .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(5L, acknowledgementBlockNumberExtractor));

        // All three blocks must be in storage — the gap at block 3 was filled by the RESEND.
        // ACK(5) was received only after block 5 was persisted, so blocks 3 and 4 (persisted
        // before block 5 in the forwarder's messaging order) are also present.
        final BlockAccessServiceInterface.BlockAccessServiceClient blockAccessClient =
                new BlockAccessServiceInterface.BlockAccessServiceClient(getBlockPbjGrpcClient, OPTIONS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(3L).build())
                        .status())
                .as("block 3 must be present in storage — gap was filled by RESEND mechanism")
                .isEqualTo(BlockResponse.Code.SUCCESS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(4L).build())
                        .status())
                .as("block 4 must be present in storage")
                .isEqualTo(BlockResponse.Code.SUCCESS);
        assertThat(blockAccessClient
                        .getBlock(BlockRequest.newBuilder().blockNumber(5L).build())
                        .status())
                .as("block 5 must be present in storage")
                .isEqualTo(BlockResponse.Code.SUCCESS);

        publisher1Client.close();
        publisher2Client.close();
        blockAccessClient.close();
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

    private PublishStreamRequest buildPublishRequest(final BlockItem[] blockItems) {
        return PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(blockItems).build())
                .build();
    }

    private void endBlock(final long blockNumber, final Pipeline<? super PublishStreamRequest> requestStream) {
        requestStream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String description)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for " + description);
    }
}
