// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.capacity;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
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

        PbjGrpcClientConfig grpcConfig =
                new PbjGrpcClientConfig(timeoutDuration, tls, Optional.empty(), "application/grpc");

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
                .filter(path -> path.toString().endsWith(".blk.gz"))
                .sorted()
                .collect(Collectors.toList());

        if (blockFiles.isEmpty()) {
            System.err.printf("No block files found in %s%n", recordingFolder);
            return;
        }

        System.out.printf("Found %d block files to stream.%n", blockFiles.size());

        CompletableFuture<Void> streamingComplete = new CompletableFuture<>();
        ResponseHandler responseHandler = new ResponseHandler(streamingComplete);
        Pipeline<PublishStreamRequest> requestPipeline = publishClient.publishBlockStream(responseHandler);

        for (Path blockFile : blockFiles) {
            try {
                streamBlockFile(blockFile, requestPipeline);
                // Optional pacing; consider removing for tight windows
                LockSupport.parkNanos(10_000_000);
            } catch (RuntimeException | ParseException e) {
                System.err.printf("Error streaming file %s due to %s%n", blockFile, e);
            }
        }

        requestPipeline.onComplete();

        try {
            streamingComplete.get(Duration.ofMinutes(5).toMillis(), TimeUnit.MILLISECONDS);
        } catch (RuntimeException | InterruptedException | ExecutionException | TimeoutException e) {
            System.err.printf("Error waiting for stream completion due to %s%n", e);
        }
    }

    private void streamBlockFile(Path blockFile, Pipeline<PublishStreamRequest> requestPipeline)
            throws IOException, ParseException {
        byte[] blockData;
        try (final var gzipInputStream = new GZIPInputStream(Files.newInputStream(blockFile))) {
            blockData = gzipInputStream.readAllBytes();
        }
        Block block = Block.PROTOBUF.parse(Bytes.wrap(blockData));
        long blockNumber = block.items().getFirst().blockHeader().number();

        List<BlockItem> items = block.items();
        System.out.printf(
                "Streaming block from file: %s (%d items)%n", blockFile.getFileName(), items.size());

        final long maxMessageSizeBytes = config.serverMaxMessageSizeBytes();
        int i = 0;
        int batchCount = 0;

        while (i < items.size()) {
            List<BlockItem> batch = new ArrayList<>();
            long currentBatchSizeBytes = 0;

            // Build batch until we approach the max message size
            while (i < items.size()) {
                BlockItem item = items.get(i);
                long itemSizeBytes = BlockItem.PROTOBUF.toBytes(item).length();

                // If adding this item would exceed max size and batch is not empty, break
                if (currentBatchSizeBytes + itemSizeBytes > maxMessageSizeBytes && !batch.isEmpty()) {
                    break;
                }

                batch.add(item);
                currentBatchSizeBytes += itemSizeBytes;
                i++;
            }

            // Send the batch
            BlockItemSet itemSet = BlockItemSet.newBuilder().blockItems(batch).build();
            PublishStreamRequest request =
                    PublishStreamRequest.newBuilder().blockItems(itemSet).build();
            // Count serialized payload bytes (consistent with server)
            Bytes requestBytes = PublishStreamRequest.PROTOBUF.toBytes(request);
            metrics.addBytes(requestBytes.length());
            requestPipeline.onNext(request);
            batchCount++;
        }

        System.out.printf(
                "Sent %d batches for block %d with size %d bytes.", batchCount, blockNumber, blockData.length);
        metrics.incrementBlocks();
        metrics.reportPeriodic();
    }

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
            System.err.printf("Error receiving response: %s%n", throwable.getMessage());
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
