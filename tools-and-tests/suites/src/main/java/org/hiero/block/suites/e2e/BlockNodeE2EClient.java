// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.suites.utils.ResponsePipelineUtils;

/// A reusable, plaintext-gRPC client for driving an _external_, already-running Block Node.
///
/// The client talks to a Block Node over HTTP/2 with TLS disabled (matching the construction used by
/// `BlockNodeAPITests#createGrpcClientForPort`). It can:
///
///   - Push real `.blk` files (protobuf-serialized [Block]) via the publish service and await one
///     acknowledgement per block; see [#pushBlocks(List)].
///   - Subscribe from a starting block and assert receipt of an expected number of blocks; see
///     [#subscribeAndAssert(long, int)].
///
/// It is usable both as a programmatic Java API (so it can be reused from the API test suites) and as a
/// standalone CLI via [#main(String[])], which is driven by environment variables and intended to be invoked
/// from a GitHub Actions step. The CLI prints the pushed/subscribed block numbers to stdout and exits with a
/// non-zero status on failure.
///
/// A `.blk` file is the plain (already gunzipped) protobuf serialization of a [Block]. Each file's block
/// number is derived from its first item's block header number (`BlockHeader.number()`).
public final class BlockNodeE2EClient implements AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(BlockNodeE2EClient.class.getName());

    /// Default per-block acknowledgement / subscription receipt timeout.
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);

    /// Request options matching the plaintext application/grpc construction used by the API suites.
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final String host;
    private final int port;
    private final PbjGrpcClient publishGrpcClient;
    private final PbjGrpcClient subscribeGrpcClient;

    private BlockNodeE2EClient(final String host, final int port) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.publishGrpcClient = createGrpcClient(host, port);
        this.subscribeGrpcClient = createGrpcClient(host, port);
    }

    /// Connects to an external Block Node over plaintext gRPC.
    ///
    /// @param host the Block Node host (e.g. `localhost`)
    /// @param port the Block Node plaintext gRPC port
    /// @return a connected client; close it with [#close()] when done
    public static BlockNodeE2EClient connect(final String host, final int port) {
        return new BlockNodeE2EClient(host, port);
    }

    /// Builds a [PbjGrpcClient] for the given host and port with TLS disabled. This mirrors
    /// `BlockNodeAPITests#createGrpcClientForPort` exactly.
    private static PbjGrpcClient createGrpcClient(final String host, final int port) {
        final Duration timeoutDuration = DEFAULT_AWAIT_TIMEOUT;
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://" + host + ":" + port)
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

    /// Pushes the given `.blk` files to the Block Node in order, awaiting one acknowledgement per block.
    ///
    /// For each file the client parses the [Block], extracts its [BlockItem] list, derives the block number
    /// from the first item's block header, sends a `blockItems` request followed by an `endOfBlock` marker,
    /// then waits for the matching acknowledgement (per-block, with a [#DEFAULT_AWAIT_TIMEOUT] timeout).
    ///
    /// @param blockFiles the plain (gunzipped) `.blk` files to publish, in publish order
    /// @return the acknowledged block numbers, in order
    /// @throws Exception if a file cannot be read/parsed, a block is rejected (END_STREAM / unexpected SKIP),
    ///     or a per-block acknowledgement times out
    public List<Long> pushBlocks(final List<Path> blockFiles) throws Exception {
        Objects.requireNonNull(blockFiles, "blockFiles");
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishGrpcClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> responseObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishClient.publishBlockStream(responseObserver);

        final List<Long> expectedBlockNumbers = new ArrayList<>(blockFiles.size());
        try {
            // ONE latch for all expected acknowledgements. The observer's onNext counts down whatever latch
            // is currently set, and setAndGetOnNextLatch installs a FRESH latch that only sees onNext calls
            // arriving after it is set. A cumulative per-block latch therefore never completes once an earlier
            // ack has already arrived (the BN acks fast). So: set the latch once for N blocks, push them all,
            // then await N acknowledgements.
            final AtomicReference<CountDownLatch> ackLatch = responseObserver.setAndGetOnNextLatch(blockFiles.size());

            for (final Path blockFile : blockFiles) {
                final List<BlockItem> items = readBlockItems(blockFile);
                if (items.isEmpty()) {
                    throw new IllegalStateException("Block file has no items: " + blockFile);
                }
                final long blockNumber = blockNumberOf(items, blockFile);
                expectedBlockNumbers.add(blockNumber);

                stream.onNext(PublishStreamRequest.newBuilder()
                        .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                        .build());
                stream.onNext(PublishStreamRequest.newBuilder()
                        .endOfBlock(
                                BlockEnd.newBuilder().blockNumber(blockNumber).build())
                        .build());

                LOGGER.log(System.Logger.Level.INFO, "Pushed block {0} from {1}", blockNumber, blockFile);
            }

            awaitLatch(ackLatch, blockFiles.size() + " acknowledgement(s)");
            return assertAllAcknowledged(responseObserver, expectedBlockNumbers);
        } finally {
            stream.closeConnection();
            publishClient.close();
        }
    }

    /// Subscribes from `startBlock` and collects exactly `expectedCount` distinct blocks.
    ///
    /// Distinct blocks are counted by the block header items received on the stream. The call asserts that at
    /// least `expectedCount` distinct block headers arrive within [#DEFAULT_AWAIT_TIMEOUT].
    ///
    /// @param startBlock the first block number to subscribe from
    /// @param expectedCount the number of distinct blocks to receive
    /// @return the received block numbers, in order of first receipt
    /// @throws Exception if the expected number of blocks is not received within the timeout
    public List<Long> subscribeAndAssert(final long startBlock, final int expectedCount) throws Exception {
        if (expectedCount <= 0) {
            throw new IllegalArgumentException("expectedCount must be positive, was " + expectedCount);
        }
        final long endBlock = startBlock + expectedCount - 1L;
        final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient subscribeClient =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(
                        subscribeGrpcClient, OPTIONS);
        final ResponsePipelineUtils<SubscribeStreamResponse> responseObserver = new ResponsePipelineUtils<>();

        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlock)
                .endBlockNumber(endBlock)
                .build();

        // subscribeBlockStream blocks until the server-streaming RPC ends, so run it off-thread and gate on
        // the receipt of expectedCount distinct block headers by polling the observer's recorded responses.
        final List<Long> receivedBlockNumbers = new ArrayList<>(expectedCount);

        final Thread subscribeThread = new Thread(
                () -> {
                    try {
                        subscribeClient.subscribeBlockStream(request, responseObserver);
                    } catch (final RuntimeException e) {
                        LOGGER.log(System.Logger.Level.WARNING, "subscribeBlockStream ended with error", e);
                    }
                },
                "bn-e2e-subscribe");
        subscribeThread.start();

        try {
            final long deadline = System.currentTimeMillis() + DEFAULT_AWAIT_TIMEOUT.toMillis();
            int scanned = 0;
            while (receivedBlockNumbers.size() < expectedCount && System.currentTimeMillis() < deadline) {
                final List<SubscribeStreamResponse> calls = responseObserver.getOnNextCalls();
                for (; scanned < calls.size(); scanned++) {
                    final SubscribeStreamResponse response = calls.get(scanned);
                    if (response.hasBlockItems()) {
                        for (final BlockItem item : response.blockItems().blockItems()) {
                            if (item.hasBlockHeader()) {
                                final long number = item.blockHeader().number();
                                if (!receivedBlockNumbers.contains(number)) {
                                    receivedBlockNumbers.add(number);
                                    LOGGER.log(System.Logger.Level.INFO, "Subscribed block {0}", number);
                                }
                            }
                        }
                    }
                }
                // Surface a server-side stream error immediately instead of spinning to a misleading
                // "Timed out" — the subscribe thread only logs onError, so the caller would otherwise never
                // see why the stream ended short.
                if (receivedBlockNumbers.size() < expectedCount
                        && !responseObserver.getOnErrorCalls().isEmpty()) {
                    final Throwable serverError =
                            responseObserver.getOnErrorCalls().getFirst();
                    throw new IllegalStateException(
                            "Subscribe stream ended with a server error after " + receivedBlockNumbers.size()
                                    + "/" + expectedCount + " blocks from block " + startBlock + ": "
                                    + serverError,
                            serverError);
                }
                if (receivedBlockNumbers.size() < expectedCount) {
                    Thread.sleep(50);
                }
            }
            if (receivedBlockNumbers.size() < expectedCount) {
                throw new IllegalStateException("Timed out waiting for " + expectedCount
                        + " blocks from block " + startBlock + "; received " + receivedBlockNumbers.size() + ": "
                        + receivedBlockNumbers);
            }
            return receivedBlockNumbers;
        } finally {
            subscribeThread.interrupt();
            subscribeClient.close();
        }
    }

    /// Reads a plain `.blk` file and returns its block items.
    private static List<BlockItem> readBlockItems(final Path blockFile) throws Exception {
        final byte[] raw = Files.readAllBytes(blockFile);
        final Block block = Block.PROTOBUF.parse(Bytes.wrap(raw));
        return block.items();
    }

    /// Derives the block number from the first (block header) item of a parsed block.
    private static long blockNumberOf(final List<BlockItem> items, final Path blockFile) {
        final BlockItem first = items.getFirst();
        if (!first.hasBlockHeader()) {
            throw new IllegalStateException("First item is not a block header in " + blockFile);
        }
        return first.blockHeader().number();
    }

    /// Asserts that every response received was an acknowledgement and that the acknowledged block numbers
    /// cover all expected blocks. Returns the acknowledged block numbers in receipt order.
    private static List<Long> assertAllAcknowledged(
            final ResponsePipelineUtils<PublishStreamResponse> responseObserver, final List<Long> expected) {
        final List<PublishStreamResponse> calls = responseObserver.getOnNextCalls();
        final List<Long> acked = new ArrayList<>(calls.size());
        for (final PublishStreamResponse response : calls) {
            final PublishStreamResponse.ResponseOneOfType kind =
                    response.response().kind();
            if (kind != PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT) {
                throw new AssertionError("Expected only acknowledgements; got " + kind + ": " + response);
            }
            acked.add(Objects.requireNonNull(response.acknowledgement()).blockNumber());
        }
        if (!acked.containsAll(expected)) {
            throw new AssertionError("Acknowledged blocks " + acked + " do not cover expected " + expected);
        }
        return acked;
    }

    private static void awaitLatch(final AtomicReference<CountDownLatch> latch, final String description)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (latch.get().getCount() != 0) {
            throw new IllegalStateException("Timed out waiting for " + description);
        }
    }

    @Override
    public void close() {
        publishGrpcClient.close();
        subscribeGrpcClient.close();
    }

    /// CLI entry point, driven by environment variables:
    ///
    ///   - `SERVER_HOST` (default `localhost`)
    ///   - `SERVER_PORT` (default `40840`)
    ///   - `BN_E2E_MODE` (default `PUSH`; one of `PUSH`, `SUBSCRIBE`, `BOTH`)
    ///   - `BN_E2E_BLOCK_FILES` (comma-separated `.blk` paths; required for PUSH/BOTH)
    ///   - `BN_E2E_START_BLOCK` (default `0`)
    ///   - `BN_E2E_EXPECT_COUNT` (default `5`)
    ///
    /// Prints pushed/subscribed block numbers to stdout and exits non-zero on failure.
    ///
    /// @param args unused; configuration is read from the environment
    public static void main(final String[] args) {
        final String host = envOrDefault("SERVER_HOST", "localhost");
        final int port = Integer.parseInt(envOrDefault("SERVER_PORT", "40840"));
        final String mode = envOrDefault("BN_E2E_MODE", "PUSH").toUpperCase();
        final String blockFilesCsv = envOrDefault("BN_E2E_BLOCK_FILES", "");
        final long startBlock = Long.parseLong(envOrDefault("BN_E2E_START_BLOCK", "0"));
        final int expectCount = Integer.parseInt(envOrDefault("BN_E2E_EXPECT_COUNT", "5"));

        LOGGER.log(System.Logger.Level.INFO, "BlockNodeE2EClient host={0} port={1} mode={2}", host, port, mode);

        try (BlockNodeE2EClient client = BlockNodeE2EClient.connect(host, port)) {
            if ("PUSH".equals(mode) || "BOTH".equals(mode)) {
                final List<Path> blockFiles = parseBlockFiles(blockFilesCsv);
                if (blockFiles.isEmpty()) {
                    throw new IllegalArgumentException("BN_E2E_BLOCK_FILES is required for mode " + mode);
                }
                final List<Long> pushed = client.pushBlocks(blockFiles);
                System.out.println("PUSHED " + pushed);
            }
            if ("SUBSCRIBE".equals(mode) || "BOTH".equals(mode)) {
                final List<Long> subscribed = client.subscribeAndAssert(startBlock, expectCount);
                System.out.println("SUBSCRIBED " + subscribed);
            }
            if (!"PUSH".equals(mode) && !"SUBSCRIBE".equals(mode) && !"BOTH".equals(mode)) {
                throw new IllegalArgumentException("Unknown BN_E2E_MODE: " + mode + " (expected PUSH|SUBSCRIBE|BOTH)");
            }
        } catch (final Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "BlockNodeE2EClient failed", e);
            System.err.println("FAILED: " + e.getMessage());
            System.exit(1);
        }
    }

    private static List<Path> parseBlockFiles(final String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Paths::get)
                .toList();
    }

    private static String envOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /// Request options record matching `BlockNodeAPITests.Options`.
    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}
}
