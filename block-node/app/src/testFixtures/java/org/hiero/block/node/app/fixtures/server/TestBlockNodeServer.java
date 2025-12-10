// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.server;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.PbjRouting.Builder;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
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
                .service((BlockStreamSubscribeServiceInterface) (request, replies) -> {
                    for (long i = request.startBlockNumber(); i <= request.endBlockNumber(); i++) {
                        if (!historicalBlockFacility.availableBlocks().contains(i)) {
                            replies.onNext(SubscribeStreamResponse.newBuilder()
                                    .status(SubscribeStreamResponse.Code.NOT_AVAILABLE)
                                    .build());
                            replies.onComplete();
                            return;
                        }

                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .blockItems(BlockItemSet.newBuilder()
                                        .blockItems(historicalBlockFacility
                                                .block(i)
                                                .block()
                                                .items())
                                        .build())
                                .build());
                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .endOfBlock(BlockEnd.newBuilder().blockNumber(i).build())
                                .build());
                    }

                    replies.onNext(SubscribeStreamResponse.newBuilder()
                            .status(Code.SUCCESS)
                            .build());
                    replies.onComplete();
                });
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

    private class TrivialBlockNodeServerInterface implements BlockNodeServiceInterface {
        private final HistoricalBlockFacility historicalBlockFacility;

        public TrivialBlockNodeServerInterface(final HistoricalBlockFacility historicalFacility) {
            historicalBlockFacility = historicalFacility;
        }

        @Override
        @NonNull
        public ServerStatusResponse serverStatus(@NonNull final ServerStatusRequest request) {
            return ServerStatusResponse.newBuilder()
                    .firstAvailableBlock(
                            historicalBlockFacility.availableBlocks().min())
                    .lastAvailableBlock(
                            historicalBlockFacility.availableBlocks().max())
                    .onlyLatestState(false)
                    .build();
        }
    }
}
