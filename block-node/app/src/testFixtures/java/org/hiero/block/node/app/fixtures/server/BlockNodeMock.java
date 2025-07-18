// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.server;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

public class BlockNodeMock {

    /** The web server. */
    private final WebServer webServer;

    private final HistoricalBlockFacility historicalBlockFacility;

    public BlockNodeMock(int port, HistoricalBlockFacility historicalBlockFacility) {
        this.historicalBlockFacility = historicalBlockFacility;
        // Override the default message size in PBJ
        final PbjConfig pbjConfig =
                PbjConfig.builder().name("pbj").maxMessageSizeBytes(4_194_304).build();

        // Create the service builder
        // Create HTTP & GRPC routing builders
        final TestFixtureServiceBuilder serviceBuilder = new TestFixtureServiceBuilder();

        serviceBuilder.registerGrpcService(new BlockNodeServiceInterface() {
            @NonNull
            @Override
            public ServerStatusResponse serverStatus(@NonNull ServerStatusRequest request) {
                return ServerStatusResponse.newBuilder()
                        .firstAvailableBlock(
                                historicalBlockFacility.availableBlocks().min())
                        .lastAvailableBlock(
                                historicalBlockFacility.availableBlocks().max())
                        .build();
            }
        });

        serviceBuilder.registerGrpcService(new BlockStreamSubscribeServiceInterface() {
            @Override
            public void subscribeBlockStream(
                    @NonNull SubscribeStreamRequest request,
                    @NonNull Pipeline<? super SubscribeStreamResponse> replies) {
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
            }
        });

        webServer = WebServerConfig.builder()
                .port(port)
                .addProtocol(pbjConfig)
                .addRouting(serviceBuilder.grpcRoutingBuilder())
                .connectionConfig(ConnectionConfig.builder()
                        .sendBufferSize(32768)
                        .receiveBufferSize(32768)
                        .build())
                .build();

        webServer.start();
    }

    public static class TestFixtureServiceBuilder implements ServiceBuilder {
        private final HttpRouting.Builder httpRoutingBuilder = HttpRouting.builder();
        private final PbjRouting.Builder pbjRoutingBuilder = PbjRouting.builder();

        @Override
        public void registerHttpService(String path, HttpService... service) {
            httpRoutingBuilder.register(path, service);
        }

        @Override
        public void registerGrpcService(@NonNull ServiceInterface service) {
            pbjRoutingBuilder.service(service);
        }

        HttpRouting.Builder httpRoutingBuilder() {
            return httpRoutingBuilder;
        }

        PbjRouting.Builder grpcRoutingBuilder() {
            return pbjRoutingBuilder;
        }
    }
}
