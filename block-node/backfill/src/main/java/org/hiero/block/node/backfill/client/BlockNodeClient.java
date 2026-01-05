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
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;
import org.hiero.block.api.BlockNodeServiceInterface;

public class BlockNodeClient {
    // Default tuning values optimized for high-throughput block streaming
    private static final int DEFAULT_2MB = 2 * 1024 * 1024;
    private static final int DEFAULT_FLOW_CONTROL_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_MAX_HEADER_LIST_SIZE = 8192;
    private static final int DEFAULT_PING_TIMEOUT_MS = 500;

    // HTTP/2 specification limits (RFC 7540)
    private static final int HTTP2_MIN_FRAME_SIZE = 16_384; // 16 KB minimum per spec
    private static final int HTTP2_MAX_FRAME_SIZE = 16_777_215; // 16 MB - 1 maximum per spec
    private static final int HTTP2_MAX_WINDOW_SIZE = Integer.MAX_VALUE; // 2^31 - 1 per spec

    // Sensible bounds for timeouts and sizes
    private static final int MIN_TIMEOUT_MS = 100;
    private static final int MAX_TIMEOUT_MS = 300_000; // 5 minutes
    private static final int MIN_BUFFER_SIZE = 1024; // 1 KB
    private static final int MAX_BUFFER_SIZE = 64 * 1024 * 1024; // 64 MB
    private static final int MIN_HEADER_LIST_SIZE = 1024; // 1 KB
    private static final int MAX_HEADER_LIST_SIZE = 1024 * 1024; // 1 MB

    // Validation predicates
    private static final IntPredicate TIMEOUT_RANGE = v -> v >= MIN_TIMEOUT_MS && v <= MAX_TIMEOUT_MS;
    private static final IntPredicate HTTP2_FRAME_SIZE_RANGE =
            v -> v >= HTTP2_MIN_FRAME_SIZE && v <= HTTP2_MAX_FRAME_SIZE;
    private static final IntPredicate HTTP2_WINDOW_SIZE_RANGE = v -> v >= 1 && v <= HTTP2_MAX_WINDOW_SIZE;
    private static final IntPredicate BUFFER_SIZE_RANGE = v -> v >= MIN_BUFFER_SIZE && v <= MAX_BUFFER_SIZE;
    private static final IntPredicate HEADER_LIST_SIZE_RANGE =
            v -> v >= MIN_HEADER_LIST_SIZE && v <= MAX_HEADER_LIST_SIZE;

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

        int connectMs = getOrDefault(tuning, GrpcWebClientTuning::connectTimeout, globalTimeoutMs, TIMEOUT_RANGE);
        int readMs = getOrDefault(tuning, GrpcWebClientTuning::readTimeout, globalTimeoutMs, TIMEOUT_RANGE);
        int pollMs = getOrDefault(tuning, GrpcWebClientTuning::pollWaitTime, globalTimeoutMs, TIMEOUT_RANGE);

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
        // HTTP/2 frame and window sizes for flow control (validated per RFC 7540)
        final int maxFrameSize =
                getOrDefault(tuning, GrpcWebClientTuning::maxFrameSize, DEFAULT_2MB, HTTP2_FRAME_SIZE_RANGE);
        final int initialWindowSize =
                getOrDefault(tuning, GrpcWebClientTuning::initialWindowSize, DEFAULT_2MB, HTTP2_WINDOW_SIZE_RANGE);
        final int flowControlTimeoutMs = getOrDefault(
                tuning, GrpcWebClientTuning::flowControlTimeout, DEFAULT_FLOW_CONTROL_TIMEOUT_MS, TIMEOUT_RANGE);
        final int maxHeaderListSize = getOrDefault(
                tuning, GrpcWebClientTuning::maxHeaderListSize, DEFAULT_MAX_HEADER_LIST_SIZE, HEADER_LIST_SIZE_RANGE);
        // HTTP/2 ping for connection keep-alive
        final boolean pingEnabled = tuning == null || tuning.pingEnabled();
        final int pingTimeoutMs =
                getOrDefault(tuning, GrpcWebClientTuning::pingTimeout, DEFAULT_PING_TIMEOUT_MS, TIMEOUT_RANGE);

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
        // gRPC buffer size for message serialization (validated within sensible bounds)
        final int initialBufferSize =
                getOrDefault(tuning, GrpcWebClientTuning::initialBufferSize, DEFAULT_2MB, BUFFER_SIZE_RANGE);

        return GrpcClientProtocolConfig.builder()
                .abortPollTimeExpired(false)
                .pollWaitTime(Duration.ofMillis(pollWaitTimeMs))
                .initBufferSize(initialBufferSize)
                .build();
    }

    private int getOrDefault(
            @Nullable GrpcWebClientTuning tuning,
            ToIntFunction<GrpcWebClientTuning> getter,
            int defaultValue,
            IntPredicate validator) {
        if (tuning == null) return defaultValue;
        int value = getter.applyAsInt(tuning);
        return validator.test(value) ? value : defaultValue;
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
