// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.http2.Http2ClientProtocolConfig;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import org.hiero.block.api.BlockNodeServiceInterface;

public class BlockNodeClient {
    // Default tuning values optimized for high-throughput block streaming
    private static final int DEFAULT_2MB = 2 * 1024 * 1024;
    private static final int DEFAULT_FLOW_CONTROL_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_MAX_HEADER_LIST_SIZE = 8192;
    private static final int DEFAULT_PING_TIMEOUT_MS = 500;

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final PbjGrpcClientConfig grpcConfig;
    private final WebClient webClient;
    private BlockStreamSubscribeUnparsedClient blockStreamSubscribeUnparsedClient;
    private BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient;
    private boolean nodeReachable;

    /**
     * Constructs a BlockNodeClient using the provided configuration.
     *
     * @param blockNodeConfig the configuration for the block node, including address and port
     * @param globalTimeoutMs the global gRPC timeout in ms (fallback when tuning values are 0)
     * @param perBlockProcessingTimeoutMs timeout for processing each block
     * @param enableTls whether to enable TLS for connections
     * @param tuning optional tuning for timeouts and HTTP/2 settings
     */
    public BlockNodeClient(
            @NonNull BackfillSourceConfig blockNodeConfig,
            int globalTimeoutMs,
            int perBlockProcessingTimeoutMs,
            boolean enableTls,
            @Nullable GrpcWebClientTuning tuning) {

        int connectMs = getOrDefault(tuning, GrpcWebClientTuning::connectTimeout, globalTimeoutMs);
        int readMs = getOrDefault(tuning, GrpcWebClientTuning::readTimeout, globalTimeoutMs);
        int pollMs = getOrDefault(tuning, GrpcWebClientTuning::pollWaitTime, globalTimeoutMs);

        Tls tls = Tls.builder().enabled(enableTls).build();
        grpcConfig = new PbjGrpcClientConfig(Duration.ofMillis(readMs), tls, Optional.of(""), "application/grpc");

        webClient = WebClient.builder()
                .baseUri("http://" + blockNodeConfig.address() + ":" + blockNodeConfig.port())
                .tls(tls)
                .protocolConfigs(List.of(buildHttp2Config(tuning), buildGrpcConfig(pollMs, tuning)))
                .connectTimeout(Duration.ofMillis(connectMs))
                .keepAlive(true)
                .build();

        initializeClient(perBlockProcessingTimeoutMs);
    }

    private Http2ClientProtocolConfig buildHttp2Config(@Nullable GrpcWebClientTuning tuning) {
        // Skip HTTP/1.1 upgrade negotiation for better performance
        final boolean priorKnowledge = tuning == null || tuning.priorKnowledge();
        // HTTP/2 frame and window sizes for flow control
        final int maxFrameSize = getOrDefault(tuning, GrpcWebClientTuning::maxFrameSize, DEFAULT_2MB);
        final int initialWindowSize = getOrDefault(tuning, GrpcWebClientTuning::initialWindowSize, DEFAULT_2MB);
        final int flowControlTimeoutMs =
                getOrDefault(tuning, GrpcWebClientTuning::flowControlTimeout, DEFAULT_FLOW_CONTROL_TIMEOUT_MS);
        final int maxHeaderListSize =
                getOrDefault(tuning, GrpcWebClientTuning::maxHeaderListSize, DEFAULT_MAX_HEADER_LIST_SIZE);
        // HTTP/2 ping for connection keep-alive
        final boolean pingEnabled = tuning == null || tuning.pingEnabled();
        final int pingTimeoutMs = getOrDefault(tuning, GrpcWebClientTuning::pingTimeout, DEFAULT_PING_TIMEOUT_MS);

        return Http2ClientProtocolConfig.builder()
                .priorKnowledge(priorKnowledge)
                .maxFrameSize(maxFrameSize)
                .initialWindowSize(initialWindowSize)
                .flowControlBlockTimeout(Duration.ofMillis(flowControlTimeoutMs))
                .maxHeaderListSize(maxHeaderListSize)
                .ping(pingEnabled)
                .pingTimeout(Duration.ofMillis(pingTimeoutMs))
                .build();
    }

    private GrpcClientProtocolConfig buildGrpcConfig(int pollWaitTimeMs, @Nullable GrpcWebClientTuning tuning) {
        // gRPC buffer size for message serialization
        final int initialBufferSize = getOrDefault(tuning, GrpcWebClientTuning::initialBufferSize, DEFAULT_2MB);

        return GrpcClientProtocolConfig.builder()
                .abortPollTimeExpired(false)
                .pollWaitTime(Duration.ofMillis(pollWaitTimeMs))
                .initBufferSize(initialBufferSize)
                .build();
    }

    private int getOrDefault(
            @Nullable GrpcWebClientTuning tuning, ToIntFunction<GrpcWebClientTuning> getter, int defaultValue) {
        if (tuning == null) return defaultValue;
        int value = getter.applyAsInt(tuning);
        return value > 0 ? value : defaultValue;
    }

    public void initializeClient(int perBlockProcessingTimeoutMs) {
        try {
            PbjGrpcClient pbjGrpcClient = new PbjGrpcClient(webClient, grpcConfig);
            blockNodeServiceClient = new BlockNodeServiceInterface.BlockNodeServiceClient(pbjGrpcClient, OPTIONS);
            blockStreamSubscribeUnparsedClient =
                    new BlockStreamSubscribeUnparsedClient(pbjGrpcClient, perBlockProcessingTimeoutMs);
            nodeReachable = true;
        } catch (IllegalArgumentException | IllegalStateException | UncheckedIOException ex) {
            nodeReachable = false;
        }
    }

    public BlockStreamSubscribeUnparsedClient getBlockstreamSubscribeUnparsedClient() {
        return blockStreamSubscribeUnparsedClient;
    }

    public BlockNodeServiceInterface.BlockNodeServiceClient getBlockNodeServiceClient() {
        return blockNodeServiceClient;
    }

    public boolean isNodeReachable() {
        return nodeReachable;
    }
}
