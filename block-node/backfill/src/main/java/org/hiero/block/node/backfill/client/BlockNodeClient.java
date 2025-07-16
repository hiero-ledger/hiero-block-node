// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import io.helidon.common.tls.Tls;
import io.helidon.webclient.grpc.GrpcClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.time.Duration;
import org.hiero.block.node.backfill.client.proto.BlockNodeConfig;

public class BlockNodeClient {
    private final BlockNodeConfig blockNodeConfig;
    private final GrpcClient grpcClient;
    private final BlockNodeServerStatusClient blockNodeServerStatusClient;
    private final BlockNodeSubscribeClient blockNodeSubscribeClient;

    public BlockNodeClient(BlockNodeConfig blockNodeConfig) {
        this.blockNodeConfig = blockNodeConfig;

        // Initialize gRPC client with the block node configuration
        this.grpcClient = GrpcClient.builder()
                .tls(Tls.builder().enabled(false).build())
                .baseUri("http://" + blockNodeConfig.address() + ":" + blockNodeConfig.port())
                .protocolConfig(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(Duration.ofSeconds(30))
                        .build())
                .keepAlive(true)
                .build();
        // Initialize clients for server status and block subscription
        this.blockNodeServerStatusClient = new BlockNodeServerStatusClient(grpcClient);
        this.blockNodeSubscribeClient = new BlockNodeSubscribeClient(grpcClient);
    }

    public BlockNodeServerStatusClient getBlockNodeServerStatusClient() {
        return blockNodeServerStatusClient;
    }

    public BlockNodeSubscribeClient getBlockNodeSubscribeClient() {
        return blockNodeSubscribeClient;
    }

    public BlockNodeConfig getBlockNodeConfig() {
        return blockNodeConfig;
    }

    public GrpcClient getGrpcClient() {
        return grpcClient;
    }
}
