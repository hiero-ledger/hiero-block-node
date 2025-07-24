// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.server;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

public class TestBlockNodeServer {

    public TestBlockNodeServer(int port, HistoricalBlockFacility historicalBlockFacility) {
        // Override the default message size in PBJ
        final PbjConfig pbjConfig =
                PbjConfig.builder().name("pbj").maxMessageSizeBytes(4_194_304).build();

        // Create the service builder
        final PbjRouting.Builder pbjRoutingBuilder = PbjRouting.builder()
                .service((BlockNodeServiceInterface) request -> ServerStatusResponse.newBuilder()
                        .firstAvailableBlock(
                                historicalBlockFacility.availableBlocks().min())
                        .lastAvailableBlock(
                                historicalBlockFacility.availableBlocks().max())
                        .build())
                .service((BlockStreamSubscribeServiceInterface) (request, replies) -> {
                    if (historicalBlockFacility
                            .availableBlocks()
                            .contains(request.startBlockNumber(), request.endBlockNumber())) {
                        for (long i = request.startBlockNumber(); i <= request.endBlockNumber(); i++) {
                            replies.onNext(SubscribeStreamResponse.newBuilder()
                                    .blockItems(BlockItemSet.newBuilder()
                                            .blockItems(historicalBlockFacility
                                                    .block(i)
                                                    .block()
                                                    .items())
                                            .build())
                                    .build());
                        }

                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .status(SubscribeStreamResponse.Code.SUCCESS)
                                .build());

                        replies.onComplete();
                    }
                });

        // start the web server with the PBJ configuration and routing
        WebServer webServer = WebServerConfig.builder()
                .port(port)
                .addProtocol(pbjConfig)
                .addRouting(pbjRoutingBuilder)
                .connectionConfig(ConnectionConfig.builder()
                        .sendBufferSize(32768)
                        .receiveBufferSize(32768)
                        .build())
                .build();

        webServer.start();
    }
}
