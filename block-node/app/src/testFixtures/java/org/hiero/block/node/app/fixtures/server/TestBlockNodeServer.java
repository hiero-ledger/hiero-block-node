// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.server;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.PbjRouting.Builder;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

public class TestBlockNodeServer {
    private final WebServer webServer;

    public TestBlockNodeServer(int port, HistoricalBlockFacility historicalBlockFacility) {
        // Override the default message size in PBJ
        final PbjConfig pbjConfig =
                PbjConfig.builder().name("pbj").maxMessageSizeBytes(4_194_304).build();

        // Create the service builder
        final Builder pbjRoutingBuilder = PbjRouting.builder()
                .service(new TrivialBlockNodeServerInterface(historicalBlockFacility))
                .service(new TestBlockStreamSubscribeService(historicalBlockFacility));
        // start the web server with the PBJ configuration and routing
        webServer = WebServerConfig.builder()
                .port(port)
                .addProtocol(pbjConfig)
                .addRouting(pbjRoutingBuilder)
                .connectionConfig(ConnectionConfig.builder()
                        .sendBufferSize(524288)
                        .receiveBufferSize(524288)
                        .build())
                .build();
        webServer.start();
    }

    /**
     * Stop the web server.
     */
    public void stop() {
        if (webServer != null) {
            webServer.stop();
        }
    }

    /**
     * Test implementation of BlockStreamSubscribeService that serves blocks from the historical facility.
     * Simulates a real block node's streaming behavior for backfill integration tests.
     */
    private static final class TestBlockStreamSubscribeService implements BlockStreamSubscribeServiceInterface {
        private final HistoricalBlockFacility historicalBlockFacility;

        private TestBlockStreamSubscribeService(@NonNull final HistoricalBlockFacility historicalBlockFacility) {
            this.historicalBlockFacility = historicalBlockFacility;
        }

        @Override
        public void subscribeBlockStream(
                @NonNull final SubscribeStreamRequest request,
                @NonNull final Pipeline<? super SubscribeStreamResponse> replies) {
            boolean blocksAvailable = true;

            for (long i = request.startBlockNumber(); i <= request.endBlockNumber(); i++) {
                if (!historicalBlockFacility.availableBlocks().contains(i)) {
                    // Path 1: Requested block not available - send NOT_AVAILABLE and abort
                    replies.onNext(SubscribeStreamResponse.newBuilder()
                            .status(SubscribeStreamResponse.Code.NOT_AVAILABLE)
                            .build());
                    blocksAvailable = false;
                    break;
                } else {
                    // Path 2: Block available - send block items followed by end-of-block marker
                    try (BlockAccessor accessor = historicalBlockFacility.block(i)) {
                        Block block = Block.PROTOBUF.parse(accessor.blockBytes(BlockAccessor.Format.PROTOBUF));
                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .blockItems(BlockItemSet.newBuilder()
                                        .blockItems(block.items())
                                        .build())
                                .build());
                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .endOfBlock(BlockEnd.newBuilder().blockNumber(i).build())
                                .build());
                    } catch (ParseException e) {
                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .status(SubscribeStreamResponse.Code.NOT_AVAILABLE)
                                .build());
                        blocksAvailable = false;
                        break;
                    }
                }
            }

            // Path 3: All requested blocks sent successfully - send SUCCESS status
            if (blocksAvailable) {
                replies.onNext(SubscribeStreamResponse.newBuilder()
                        .status(Code.SUCCESS)
                        .build());
            }
            replies.onComplete();
        }
    }

    /**
     * Test implementation of BlockNodeService that returns server status from the historical facility.
     * Reports the available block range for backfill source discovery.
     */
    private static class TrivialBlockNodeServerInterface implements BlockNodeServiceInterface {
        private final HistoricalBlockFacility historicalBlockFacility;

        public TrivialBlockNodeServerInterface(final HistoricalBlockFacility historicalFacility) {
            historicalBlockFacility = historicalFacility;
        }

        @Override
        @NonNull
        public ServerStatusResponse serverStatus(@NonNull final ServerStatusRequest request) {
            // Returns the available block range from the historical facility
            return ServerStatusResponse.newBuilder()
                    .firstAvailableBlock(
                            historicalBlockFacility.availableBlocks().min())
                    .lastAvailableBlock(
                            historicalBlockFacility.availableBlocks().max())
                    .onlyLatestState(false)
                    .build();
        }

        @Override
        @NonNull
        public @org.jspecify.annotations.NonNull ServerStatusDetailResponse serverStatusDetail(
                @org.jspecify.annotations.NonNull ServerStatusRequest request) {
            return null;
        }
    }
}
