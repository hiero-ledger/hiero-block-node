// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss.client;

import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.GrpcCompression;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.http2.Http2ClientProtocolConfig;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.node.roster.bootstrap.tss.BlockNodeSourceConfig;
import org.hiero.block.node.roster.bootstrap.tss.GrpcWebClientTuning;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

public class BlockNodeClient implements Closeable {
    private static final Logger LOGGER = System.getLogger(BlockNodeClient.class.getName());
    // Default tuning values optimized for high-throughput block streaming
    private static final int DEFAULT_2MB = 2 * 1024 * 1024;
    private static final int DEFAULT_TIMEOUT_MS = 10_000; // 10 seconds
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
    private static final int MIN_HEADER_LIST_SIZE_BOUND = 1024; // 1 KB
    private static final int MAX_HEADER_LIST_SIZE_BOUND = 1024 * 1024; // 1 MB

    // Timeout config specs
    private static final IntConfigSpec CONNECT_TIMEOUT = new IntConfigSpec(
            "connectTimeout", DEFAULT_TIMEOUT_MS, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, GrpcWebClientTuning::connectTimeout);
    private static final IntConfigSpec READ_TIMEOUT = new IntConfigSpec(
            "readTimeout", DEFAULT_TIMEOUT_MS, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, GrpcWebClientTuning::readTimeout);
    private static final IntConfigSpec POLL_WAIT_TIME = new IntConfigSpec(
            "pollWaitTime", DEFAULT_TIMEOUT_MS, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, GrpcWebClientTuning::pollWaitTime);
    private static final IntConfigSpec FLOW_CONTROL_TIMEOUT = new IntConfigSpec(
            "flowControlTimeout",
            DEFAULT_TIMEOUT_MS,
            MIN_TIMEOUT_MS,
            MAX_TIMEOUT_MS,
            GrpcWebClientTuning::flowControlTimeout);
    private static final IntConfigSpec PING_TIMEOUT = new IntConfigSpec(
            "pingTimeout", DEFAULT_PING_TIMEOUT_MS, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, GrpcWebClientTuning::pingTimeout);

    // HTTP/2 config specs
    private static final IntConfigSpec MAX_FRAME_SIZE = new IntConfigSpec(
            "maxFrameSize", DEFAULT_2MB, HTTP2_MIN_FRAME_SIZE, HTTP2_MAX_FRAME_SIZE, GrpcWebClientTuning::maxFrameSize);
    private static final IntConfigSpec INITIAL_WINDOW_SIZE = new IntConfigSpec(
            "initialWindowSize", DEFAULT_2MB, 1, HTTP2_MAX_WINDOW_SIZE, GrpcWebClientTuning::initialWindowSize);
    private static final IntConfigSpec MAX_HEADER_LIST_SIZE = new IntConfigSpec(
            "maxHeaderListSize",
            DEFAULT_MAX_HEADER_LIST_SIZE,
            MIN_HEADER_LIST_SIZE_BOUND,
            MAX_HEADER_LIST_SIZE_BOUND,
            GrpcWebClientTuning::maxHeaderListSize);

    // gRPC config specs
    private static final IntConfigSpec INITIAL_BUFFER_SIZE = new IntConfigSpec(
            "initialBufferSize", DEFAULT_2MB, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, GrpcWebClientTuning::initialBufferSize);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final PbjGrpcClientConfig grpcConfig;
    private final WebClient webClient;
    private BlockNodeServiceInterface.BlockNodeServiceClient blockNodeServiceClient;
    private boolean nodeReachable;

    @Override
    public void close() throws IOException {
        if (blockNodeServiceClient != null) {
            blockNodeServiceClient.close();
        }
    }

    // Config specification record bundling name, default, range, and getter
    record IntConfigSpec(
            String name, int defaultValue, int minValue, int maxValue, ToIntFunction<GrpcWebClientTuning> getter) {
        boolean isValid(int value) {
            return value >= minValue && value <= maxValue;
        }

        int getValidOrDefault(@Nullable GrpcWebClientTuning tuning) {
            // if null use default
            if (tuning == null) return defaultValue;
            // get value from tuning
            int value = getter.applyAsInt(tuning);
            // if 0 use default
            if (value == 0) return defaultValue;
            // validate range
            if (isValid(value)) return value;
            // log warning and use default
            LOGGER.log(
                    Level.WARNING,
                    "Invalid tuning value for {0}: {1} is outside valid range [{2}, {3}], using default: {4}",
                    name,
                    value,
                    minValue,
                    maxValue,
                    defaultValue);
            return defaultValue;
        }
    }

    /// Constructs a BlockNodeClient using the provided configuration.
    ///
    /// @param blockNodeConfig the configuration for the block node, including address and port
    /// latch await
    /// @param enableTls whether to enable TLS for connections
    /// @param maxIncomingBufferSize the maximum incoming buffer size in bytes for gRPC message reception
    /// @param tuning optional tuning for timeouts and HTTP/2 settings
    public BlockNodeClient(
            @NonNull BlockNodeSourceConfig blockNodeConfig,
            boolean enableTls,
            int maxIncomingBufferSize,
            @Nullable GrpcWebClientTuning tuning) {

        Tls tls = Tls.builder().enabled(enableTls).build();
        String protocol = enableTls ? "https://" : "http://";
        grpcConfig = new PbjGrpcClientConfig(
                Duration.ofMillis(READ_TIMEOUT.getValidOrDefault(tuning)),
                tls,
                Optional.of(""),
                "application/grpc",
                GrpcCompression.IDENTITY,
                GrpcCompression.getDecompressorNames(),
                BlockAccessor.MAX_BLOCK_SIZE_BYTES,
                maxIncomingBufferSize);

        webClient = WebClient.builder()
                .baseUri(protocol + blockNodeConfig.address() + ":" + blockNodeConfig.port())
                .tls(tls)
                .protocolConfigs(List.of(
                        buildHttp2Config(tuning), buildGrpcConfig(POLL_WAIT_TIME.getValidOrDefault(tuning), tuning)))
                .connectTimeout(Duration.ofMillis(CONNECT_TIMEOUT.getValidOrDefault(tuning)))
                .keepAlive(true)
                .build();

        initializeClient();
    }

    private Http2ClientProtocolConfig buildHttp2Config(@Nullable GrpcWebClientTuning tuning) {
        // Use HTTP/1.1 upgrade by default (priorKnowledge=false) to match CN client behavior.
        // Direct h2c (priorKnowledge=true) has compatibility issues with Helidon's HTTP/2 preface timeout.
        final boolean priorKnowledge = tuning != null && tuning.priorKnowledge();
        // HTTP/2 frame and window sizes for flow control (validated per RFC 7540)
        final int maxFrameSize = MAX_FRAME_SIZE.getValidOrDefault(tuning);
        final int initialWindowSize = INITIAL_WINDOW_SIZE.getValidOrDefault(tuning);
        final int flowControlTimeoutMs = FLOW_CONTROL_TIMEOUT.getValidOrDefault(tuning);
        final int maxHeaderListSize = MAX_HEADER_LIST_SIZE.getValidOrDefault(tuning);
        // HTTP/2 ping for connection keep-alive
        final boolean pingEnabled = tuning == null || tuning.pingEnabled();
        final int pingTimeoutMs = PING_TIMEOUT.getValidOrDefault(tuning);

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
        final int initialBufferSize = INITIAL_BUFFER_SIZE.getValidOrDefault(tuning);

        return GrpcClientProtocolConfig.builder()
                .abortPollTimeExpired(false)
                .pollWaitTime(Duration.ofMillis(pollWaitTimeMs))
                .initBufferSize(initialBufferSize)
                .build();
    }

    private void initializeClient() {
        try {
            PbjGrpcClient pbjGrpcClient = new PbjGrpcClient(webClient, grpcConfig);
            blockNodeServiceClient = new BlockNodeServiceInterface.BlockNodeServiceClient(pbjGrpcClient, OPTIONS);
            nodeReachable = true;
        } catch (IllegalArgumentException | IllegalStateException | UncheckedIOException ex) {
            LOGGER.log(Level.WARNING, "Failed to initialize gRPC client: %s".formatted(ex.getMessage()), ex);
            nodeReachable = false;
        }
    }

    public BlockNodeServiceInterface.BlockNodeServiceClient getBlockNodeServiceClient() {
        return blockNodeServiceClient;
    }

    public boolean isNodeReachable() {
        return nodeReachable;
    }
}
