// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.capacity;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http2.Http2Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.tools.config.HelidonWebServerConfig;

public class NetworkCapacityServer {
    private final int port;
    private final HelidonWebServerConfig config;
    private WebServer webServer;
    private final ThroughputMetrics metrics;

    public NetworkCapacityServer(int port, HelidonWebServerConfig config) {
        this.port = port;
        this.config = config;
        this.metrics = new ThroughputMetrics("SERVER", 1);
    }

    public void start() {
        System.out.println("Starting Network Capacity Server on port " + port);
        // No explicit start; metrics begins on first data arrival.

        final PbjConfig pbjConfig = PbjConfig.builder()
                .name("pbj")
                .maxMessageSizeBytes(config.maxMessageSizeBytes())
                .build();

        final PbjRouting.Builder pbjRoutingBuilder = PbjRouting.builder();
        pbjRoutingBuilder.service((BlockStreamPublishServiceInterface)
                (replies) -> new StreamHandler((Pipeline<PublishStreamResponse>) replies));

        Http2Config h2 = Http2Config.builder()
                .initialWindowSize(config.initialWindowSize())
                .maxFrameSize(config.maxFrameSize())
                .maxConcurrentStreams(config.maxConcurrentStreams())
                .flowControlTimeout(Duration.ofMillis(config.flowControlTimeoutMillis()))
                .build();

        webServer = WebServerConfig.builder()
                .port(port)
                .addProtocol(pbjConfig)
                .addProtocol(h2)
                .addRouting(pbjRoutingBuilder)
                .connectionConfig(ConnectionConfig.builder()
                        .sendBufferSize(config.sendBufferSize())
                        .receiveBufferSize(config.receiveBufferSize())
                        .tcpNoDelay(config.tcpNoDelay())
                        .build())
                .build();

        webServer.start();
        System.out.println("Network Capacity Server started on port " + port);
    }

    public void stop() {
        System.out.println("Shutting down Network Capacity Server on port " + port);
        // Do not report final here; each stream reports its own final cleanly.
        if (webServer != null) {
            webServer.stop();
        }
    }

    private class StreamHandler implements Pipeline<PublishStreamRequest> {
        private final Pipeline<PublishStreamResponse> replies;
        private Flow.Subscription subscription;
        private long currentBlockNumber = 0;
        private final List<BlockItem> currentBlockItems = new ArrayList<>();
        private boolean hasReceivedBlockHeader = false;

        public StreamHandler(Pipeline<PublishStreamResponse> replies) {
            this.replies = replies;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
            System.out.println("Client connected - ready to receive");
        }

        @Override
        public void clientEndStreamReceived() {
            Pipeline.super.clientEndStreamReceived();
            System.out.println("Client half-closed the stream");
            // Do not report final here; we finalize in onComplete() to ensure replies are sent.
        }

        @Override
        public void onNext(final PublishStreamRequest req) {
            try {
                // Count serialized payload bytes as received
                Bytes requestBytes = PublishStreamRequest.PROTOBUF.toBytes(req);
                metrics.addBytes(requestBytes.length());
                metrics.reportPeriodic();

                BlockItemSet blockItems = req.blockItems();
                if (!blockItems.blockItems().isEmpty()) {
                    // Detect start of a block
                    if (blockItems.blockItems().getFirst().hasBlockHeader()) {
                        currentBlockNumber =
                                blockItems.blockItems().getFirst().blockHeader().number();
                        hasReceivedBlockHeader = true;
                        currentBlockItems.clear();
                        System.out.println("[SERVER] Receiving block " + currentBlockNumber);
                    }

                    // Accumulate items
                    currentBlockItems.addAll(blockItems.blockItems());

                    // Detect end of a block
                    if (blockItems.blockItems().getLast().hasBlockProof() && hasReceivedBlockHeader) {
                        metrics.incrementBlocks();
                        System.out.printf(
                                "[SERVER] Completed block %d (Total %d).",
                                currentBlockNumber, metrics.getTotalBlocks());

                        // Send acknowledgement
                        PublishStreamResponse response = PublishStreamResponse.newBuilder()
                                .acknowledgement(PublishStreamResponse.BlockAcknowledgement.newBuilder()
                                        .blockNumber(currentBlockNumber)
                                        .build())
                                .build();
                        replies.onNext(response);

                        // Reset for next block
                        currentBlockItems.clear();
                        hasReceivedBlockHeader = false;
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing request: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            System.err.println("Error in stream: " + throwable.getMessage());
            metrics.reportFinal();
        }

        @Override
        public void onComplete() {
            System.out.println("Stream completed");
            metrics.reportFinal(); // Single, authoritative final report for this stream
        }
    }
}
