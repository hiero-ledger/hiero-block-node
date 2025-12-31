// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.UncheckedIOException;
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
    private final PbjGrpcClientConfig grpcConfig;
    private final WebClient webClient;
    private BlockStreamSubscribeUnparsedClient blockStreamSubscribeUnparsedClient;
    private BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient;
    private boolean nodeReachable;

    /**
     * Constructs a BlockNodeClient using the provided configuration.
     *
     * @param blockNodeConfig the configuration for the block node, including address and port
     */
    public BlockNodeClient(
            BackfillSourceConfig blockNodeConfig, int timeoutMs, int perBlockProcessingTimeoutMs, boolean enableTls) {

        final Duration timeoutDuration = Duration.ofMillis(timeoutMs);

        final Tls tls = Tls.builder().enabled(enableTls).build();
        grpcConfig = new PbjGrpcClientConfig(timeoutDuration, tls, Optional.of(""), "application/grpc");

        webClient = WebClient.builder()
                .baseUri("http://" + blockNodeConfig.address() + ":" + blockNodeConfig.port())
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(timeoutDuration)
                        .build()))
                .connectTimeout(timeoutDuration)
                .build();

        initializeClient(perBlockProcessingTimeoutMs);
    }

    public void initializeClient(int perBlockProcessingTimeoutMs) {
        try {
            PbjGrpcClient pbjGrpcClient = new PbjGrpcClient(webClient, grpcConfig);

            // we reuse the host connection with many services.
            blockNodeServiceClient = new BlockNodeServiceInterface.BlockNodeServiceClient(pbjGrpcClient, OPTIONS);
            this.blockStreamSubscribeUnparsedClient =
                    new BlockStreamSubscribeUnparsedClient(pbjGrpcClient, perBlockProcessingTimeoutMs);
            nodeReachable = true;
        } catch (IllegalArgumentException | IllegalStateException | UncheckedIOException ex) {
            // unable to setup clients
            nodeReachable = false;
        }
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

    public boolean isNodeReachable() {
        return nodeReachable;
    }
}
