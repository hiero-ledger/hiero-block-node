// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.hiero.block.api.BlockNodeServiceInterface;

public class BlockNodeClient {
    // Options definition for all gRPC services in the block node client
    private static record Options(Optional<String> authority, String contentType)
            implements ServiceInterface.RequestOptions {}

    private static final BlockNodeClient.Options OPTIONS =
            new BlockNodeClient.Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    // block node services
    private final BlockStreamSubscribeUnparsedClient blockStreamSubscribeUnparsedClient;
    private final BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient;

    /**
     * Constructs a BlockNodeClient using the provided configuration.
     *
     * @param blockNodeConfig the configuration for the block node, including address and port
     * @param connectionTimeoutMs connection timeout in milliseconds
     * @param readTimeoutMs read timeout in milliseconds
     * @param pollWaitTimeMs poll wait time in milliseconds
     * @param enableTls whether to enable TLS for secure connections
     */
    public BlockNodeClient(BackfillSourceConfig blockNodeConfig, int connectionTimeoutMs, int readTimeoutMs, int pollWaitTimeMs, boolean enableTls) {

        final Duration connectTimeout = Duration.ofMillis(connectionTimeoutMs);
        final Duration readTimeout = Duration.ofMillis(readTimeoutMs);
        final Duration pollWaitTime = Duration.ofMillis(pollWaitTimeMs);

        final Tls tls = Tls.builder().enabled(enableTls).build();
        final PbjGrpcClientConfig grpcConfig =
                new PbjGrpcClientConfig(readTimeout, tls, Optional.of(""), "application/grpc");

        final WebClient webClient = WebClient.builder()
                .baseUri("http://" + blockNodeConfig.address() + ":" + blockNodeConfig.port())
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(pollWaitTime)
                        .build()))
                .connectTimeout(connectTimeout)
                .build();

        PbjGrpcClient pbjGrpcClient = new PbjGrpcClient(webClient, grpcConfig);

        // we reuse the host connection with many services.
        blockNodeServiceClient = new BlockNodeServiceInterface.BlockNodeServiceClient(pbjGrpcClient, OPTIONS);
        this.blockStreamSubscribeUnparsedClient = new BlockStreamSubscribeUnparsedClient(pbjGrpcClient);
    }

    /**
     * Returns the BlockStreamSubscribeUnparsedClient for subscribing to block streams.
     *
     * @return the BlockStreamSubscribeUnparsedClient
     */
    public BlockStreamSubscribeUnparsedClient getBlockstreamSubscribeUnparsedClient() {
        return blockStreamSubscribeUnparsedClient;
    }

    /**
     * Returns the BlockNodeServiceClient for accessing block node services.
     *
     * @return the BlockNodeServiceClient
     */
    public BlockNodeServiceInterface.BlockNodeServiceClient getBlockNodeServiceClient() {
        return blockNodeServiceClient;
    }
}
