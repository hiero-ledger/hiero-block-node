// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.capacity;

import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http2.Http2Config;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Flow;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
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
        System.out.printf("Starting Network Capacity Server on port %d%n", port);
        // No explicit start; metrics begins on first data arrival.

        final PbjConfig pbjConfig = PbjConfig.builder()
                .name("pbj")
                .maxMessageSizeBytes(config.maxMessageSizeBytes())
                .build();

        final PbjRouting.Builder pbjRoutingBuilder = PbjRouting.builder();
        pbjRoutingBuilder.service(new TestPublishInterface());

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
                .connectionOptions(SocketOptions.builder()
                        .socketSendBufferSize(config.sendBufferSize())
                        .socketReceiveBufferSize(config.receiveBufferSize())
                        .tcpNoDelay(config.tcpNoDelay())
                        .build())
                .backlog(config.backlogSize())
                .writeQueueLength(config.writeQueueLength())
                .build();

        webServer.start();
        System.out.printf("Network Capacity Server started on port %d%n", port);
    }

    public void stop() {
        System.out.printf("Shutting down Network Capacity Server on port %d%n", port);
        // Do not report final here; each stream reports its own final cleanly.
        if (webServer != null) {
            webServer.stop();
        }
    }

    /// Implements the publish service using the same open() + unparsed-request pattern as the real
    /// BN StreamPublisherPlugin, avoiding full BlockItem deserialization on the server side.
    private class TestPublishInterface implements BlockStreamPublishServiceInterface {
        @Override
        @NonNull
        public Pipeline<? super Bytes> open(
                @NonNull final Method method,
                @NonNull final RequestOptions options,
                @NonNull final Pipeline<? super Bytes> replies) {
            return Pipelines.<PublishStreamRequestUnparsed, PublishStreamResponse>bidiStreaming()
                    .mapRequest(bytes -> PublishStreamRequestUnparsed.PROTOBUF.parse(
                            bytes.toReadableSequentialData(),
                            false,
                            true,
                            config.maxMessageSizeBytes() / 8,
                            config.maxMessageSizeBytes()))
                    .method(StreamHandler::new)
                    .respondTo(replies)
                    .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                    .build();
        }
    }

    private class StreamHandler implements Pipeline<PublishStreamRequestUnparsed> {
        private final Pipeline<? super PublishStreamResponse> replies;
        private long currentBlockNumber = 0;
        private boolean hasReceivedBlockHeader = false;

        public StreamHandler(Pipeline<? super PublishStreamResponse> replies) {
            this.replies = replies;
            System.out.println("Client connected - ready to receive");
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            // Subscription demand is managed by the Pipelines framework
        }

        @Override
        public void onNext(final PublishStreamRequestUnparsed req) {
            try {
                int requestLength = PublishStreamRequestUnparsed.PROTOBUF.measureRecord(req);
                metrics.addBytes(requestLength);
                metrics.reportPeriodic();

                if (!req.hasBlockItems()) return;

                List<BlockItemUnparsed> items = req.blockItems().blockItems();
                if (items.isEmpty()) return;

                if (items.getFirst().hasBlockHeader()) {
                    currentBlockNumber = parseBlockNumber(items.getFirst());
                    hasReceivedBlockHeader = true;
                    System.out.printf("[SERVER] Receiving block %d%n", currentBlockNumber);
                }

                if (items.getLast().hasBlockProof() && hasReceivedBlockHeader) {
                    metrics.incrementBlocks();
                    System.out.printf(
                            "[SERVER] Completed block %d (Total %d).%n", currentBlockNumber, metrics.getTotalBlocks());

                    PublishStreamResponse response = PublishStreamResponse.newBuilder()
                            .acknowledgement(PublishStreamResponse.BlockAcknowledgement.newBuilder()
                                    .blockNumber(currentBlockNumber)
                                    .build())
                            .build();
                    replies.onNext(response);
                    hasReceivedBlockHeader = false;
                }
            } catch (RuntimeException e) {
                if (socketClosed(e)) {
                    System.out.println("Client connection closed; generating final throughput report.");
                    metrics.reportFinal();
                    return;
                }
                System.err.printf("Error processing request: %s%n", e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            System.err.printf("Error in stream: %s%n", throwable.getMessage());
            metrics.reportFinal();
        }

        @Override
        public void onComplete() {
            // Brief pause so the last ACK write can flush before the response pipeline closes.
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Stream completed");
            metrics.reportFinal();
        }

        private long parseBlockNumber(BlockItemUnparsed headerItem) {
            try {
                BlockHeader header = standardParse(BlockHeader.PROTOBUF, headerItem.blockHeaderOrThrow());
                return header.number();
            } catch (ParseException e) {
                return currentBlockNumber + 1;
            }
        }

        private boolean socketClosed(Throwable throwable) {
            Throwable current = throwable;
            while (current != null) {
                if (current instanceof SocketException socket
                        && "Socket closed".equalsIgnoreCase(socket.getMessage())) {
                    return true;
                }
                current = current.getCause();
            }
            return false;
        }
    }
}
