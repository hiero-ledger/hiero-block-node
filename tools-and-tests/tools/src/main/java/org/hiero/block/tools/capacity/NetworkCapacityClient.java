// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.capacity;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.http2.Http2ClientProtocolConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.tools.blocks.validation.ProtobufParsingConstants;
import org.hiero.block.tools.config.HelidonWebClientConfig;

public class NetworkCapacityClient {
    private final HelidonWebClientConfig config;
    private final Path recordingFolder;
    private final ThroughputMetrics metrics;
    private final String serverAddress;
    private final int serverPort;

    public NetworkCapacityClient(
            HelidonWebClientConfig config, Path recordingFolder, String serverAddress, int serverPort) {
        this.config = config;
        this.recordingFolder = recordingFolder;
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.metrics = new ThroughputMetrics("CLIENT", 1);
    }

    public void run() throws IOException {
        System.out.println("Starting Network Capacity Client");
        System.out.printf("Recording folder %s%n", recordingFolder);

        // No explicit start; metrics starts on first data seen.
        BlockStreamPublishClient publishClient = createPublishClient();
        streamBlocksFromFolder(publishClient);

        metrics.reportFinal();
    }

    private BlockStreamPublishClient createPublishClient() {
        Duration timeoutDuration = Duration.ofMillis(config.readTimeoutMillis());
        Tls tls = Tls.builder().enabled(false).build();

        String encoding = config.grpcEncoding().isBlank() ? "identity" : config.grpcEncoding();
        PbjGrpcClientConfig grpcConfig = new PbjGrpcClientConfig(
                timeoutDuration, tls, Optional.empty(), "application/grpc", encoding, Set.of("identity", "gzip"));

        Http2ClientProtocolConfig http2Config = Http2ClientProtocolConfig.builder()
                .flowControlBlockTimeout(Duration.ofMillis(config.flowControlTimeoutMillis()))
                .initialWindowSize(config.initialWindowSize())
                .maxFrameSize(config.maxFrameSize())
                .maxHeaderListSize(config.maxHeaderListSize())
                .ping(config.pingEnabled())
                .pingTimeout(Duration.ofMillis(config.pingTimeoutMillis()))
                .priorKnowledge(config.priorKnowledge())
                .build();

        GrpcClientProtocolConfig grpcProtocolConfig = GrpcClientProtocolConfig.builder()
                .abortPollTimeExpired(false)
                .pollWaitTime(timeoutDuration)
                .build();

        WebClient webClient = WebClient.builder()
                .baseUri("http://" + serverAddress + ":" + serverPort)
                .tls(tls)
                .protocolConfigs(List.of(http2Config, grpcProtocolConfig))
                .connectTimeout(timeoutDuration)
                .build();

        PbjGrpcClient pbjGrpcClient = new PbjGrpcClient(webClient, grpcConfig);
        return new BlockStreamPublishClient(pbjGrpcClient);
    }

    private void streamBlocksFromFolder(BlockStreamPublishClient publishClient) throws IOException {
        List<Path> blockFiles = Files.list(recordingFolder)
                .filter(path ->
                        path.toString().endsWith(".blk.gz") || path.toString().endsWith(".blk"))
                .sorted()
                .collect(Collectors.toList());

        if (blockFiles.isEmpty()) {
            System.err.printf("No block files found in %s%n", recordingFolder);
            return;
        }

        System.out.printf("Found %d block files to stream.%n", blockFiles.size());

        // Phase 1: Pre-build all batches before opening the gRPC stream.
        // This separates parsing overhead from network measurement time, emulating how the
        // real CN generates live blocks rather than reading from disk during streaming.
        System.out.println("Preparing block batches (parsing block files)...");
        List<PreparedBlock> preparedBlocks = new ArrayList<>();
        for (Path blockFile : blockFiles) {
            try {
                PreparedBlock prepared = prepareBlockBatches(blockFile);
                preparedBlocks.add(prepared);
                System.out.printf(
                        "Prepared block %d from %s: %d batches, %d bytes%n",
                        prepared.blockNumber(),
                        blockFile.getFileName(),
                        prepared.batches().size(),
                        prepared.rawDataSizeBytes());
            } catch (RuntimeException | ParseException e) {
                System.err.printf("Error preparing file %s due to %s%n", blockFile, e);
            }
        }
        System.out.printf("All %d blocks prepared. Starting network stream.%n", preparedBlocks.size());

        // Phase 2: Open gRPC connection and stream all pre-built batches.
        CompletableFuture<Void> streamingComplete = new CompletableFuture<>();
        ResponseHandler responseHandler = new ResponseHandler(streamingComplete);
        Pipeline<PublishStreamRequest> requestPipeline = publishClient.publishBlockStream(responseHandler);

        for (PreparedBlock prepared : preparedBlocks) {
            System.out.printf(
                    "Streaming block %d (%d batches, %d bytes)%n",
                    prepared.blockNumber(), prepared.batches().size(), prepared.rawDataSizeBytes());

            for (PreparedBatch batch : prepared.batches()) {
                metrics.addBytes(batch.sizeBytes());
                requestPipeline.onNext(batch.request());
            }

            LockSupport.parkNanos(config.delayBetweenBlocksMs() * 1_000_000L);
            metrics.incrementBlocks();
            metrics.reportPeriodic();
        }

        // Wait for all ACKs to arrive before half-closing the send side.
        // The last ACK is in-flight from the server while we finish the send loop.
        long ackDeadline = System.currentTimeMillis() + 2_000;
        while (metrics.getTotalBlocksAcked() < metrics.getTotalBlocks() && System.currentTimeMillis() < ackDeadline) {
            LockSupport.parkNanos(10_000_000L);
        }
        requestPipeline.onComplete();

        try {
            streamingComplete.get(Duration.ofMinutes(5).toMillis(), TimeUnit.MILLISECONDS);
        } catch (RuntimeException | InterruptedException | ExecutionException | TimeoutException e) {
            System.err.printf("Error waiting for stream completion due to %s%n", e);
        }
    }

    private PreparedBlock prepareBlockBatches(Path blockFile) throws IOException, ParseException {
        byte[] blockData;
        if (blockFile.toString().endsWith(".blk.gz")) {
            try (final var gzipInputStream = new GZIPInputStream(Files.newInputStream(blockFile))) {
                blockData = gzipInputStream.readAllBytes();
            }
        } else {
            blockData = Files.readAllBytes(blockFile);
        }

        Block block = Block.PROTOBUF.parse(
                Bytes.wrap(blockData).toReadableSequentialData(),
                false,
                false,
                Codec.DEFAULT_MAX_DEPTH,
                ProtobufParsingConstants.MAX_MESSAGE_SIZE);

        long blockNumber = block.items().getFirst().blockHeader().number();
        List<BlockItem> items = block.items();
        final long maxMessageSizeBytes = config.serverMaxMessageSizeBytes();
        List<PreparedBatch> batches = new ArrayList<>();

        for (int i = 0; i < items.size(); ) {
            List<BlockItem> batch = new ArrayList<>();
            long currentBatchSizeBytes = 0;

            while (i < items.size()) {
                BlockItem item = items.get(i);
                int itemSizeBytes = BlockItem.PROTOBUF.measureRecord(item);

                if (!batch.isEmpty() && currentBatchSizeBytes + itemSizeBytes > maxMessageSizeBytes) {
                    break;
                }

                batch.add(item);
                currentBatchSizeBytes += itemSizeBytes;
                i++;
            }

            BlockItemSet itemSet = BlockItemSet.newBuilder().blockItems(batch).build();
            PublishStreamRequest request =
                    PublishStreamRequest.newBuilder().blockItems(itemSet).build();
            int requestSize = PublishStreamRequest.PROTOBUF.measureRecord(request);
            batches.add(new PreparedBatch(request, requestSize));
        }

        return new PreparedBlock(blockNumber, blockData.length, batches);
    }

    private record PreparedBlock(long blockNumber, int rawDataSizeBytes, List<PreparedBatch> batches) {}

    private record PreparedBatch(PublishStreamRequest request, int sizeBytes) {}

    private class ResponseHandler implements Pipeline<PublishStreamResponse> {
        private final CompletableFuture<Void> completionFuture;

        public ResponseHandler(CompletableFuture<Void> completionFuture) {
            this.completionFuture = completionFuture;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            System.out.println("Connected to server, starting stream");
        }

        @Override
        public void onNext(PublishStreamResponse response) {
            if (response.hasAcknowledgement()) {
                long blockNumber = response.acknowledgement().blockNumber();
                metrics.incrementAckedBlocks();
                System.out.printf(
                        "[CLIENT] ACK for block %d (Total ACKed: %d/%d)%n",
                        blockNumber, metrics.getTotalBlocksAcked(), metrics.getTotalBlocks());
                metrics.reportPeriodic();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            System.err.printf("Error receiving response: %s.%n", throwable);
            completionFuture.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            System.out.println("Stream completed successfully");
            completionFuture.complete(null);
        }
    }

    private static class BlockStreamPublishClient {
        private final PbjGrpcClient pbjGrpcClient;
        private final ServiceInterface.RequestOptions OPTIONS;

        public BlockStreamPublishClient(PbjGrpcClient pbjGrpcClient) {
            this.pbjGrpcClient = pbjGrpcClient;
            this.OPTIONS = new RequestOptions(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);
        }

        public Pipeline<PublishStreamRequest> publishBlockStream(Pipeline<PublishStreamResponse> responses) {
            BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient client =
                    new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(pbjGrpcClient, OPTIONS);
            return (Pipeline<PublishStreamRequest>) client.publishBlockStream(responses);
        }

        private static record RequestOptions(Optional<String> authority, String contentType)
                implements ServiceInterface.RequestOptions {}
    }
}
