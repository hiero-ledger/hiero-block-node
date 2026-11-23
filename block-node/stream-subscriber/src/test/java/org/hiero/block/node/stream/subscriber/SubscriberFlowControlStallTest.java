// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.BlockingClientCall;
import io.grpc.stub.ClientCalls;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http2.Http2Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Serves the real subscriber plugin over a real Helidon and PBJ HTTP/2 server, configured like the
/// block node, to a client that stops reading, as the mirror node importer does while it processes
/// a block. The client never returns flow-control window, so the session blocks writing until
/// Helidon's outbound flow-control timeout fails the write and the call ends.
///
/// PBJ ends the call with `grpc-status: 13` when the write fails. The session used to complete the
/// response pipeline afterwards, which with PBJ before 0.16.4 wrote a second trailers frame with
/// `grpc-status: 0`, and the client reset the already closed stream with `STREAM_CLOSED`. These
/// tests also fail that way if this module is built against PBJ 0.16.2 without the session fix.
@DisplayName("Subscriber session against a client that stops reading")
@Timeout(60)
class SubscriberFlowControlStallTest
        extends GrpcPluginTestBase<
                SubscriberServicePlugin,
                SubscriberFlowControlStallTest.CompletionTrackingExecutor,
                ScheduledBlockingExecutor> {
    /// Short enough to keep the tests fast. Helidon checks it only after each backoff sleep
    /// (50 ms doubling to 5 s), so a 1 s limit fails the write about 1.55 s into the stall. With the
    /// block node default of 10 s it fails about 11.35 s into the stall.
    private static final Duration FLOW_CONTROL_TIMEOUT = Duration.ofMillis(1_000);
    /// Room for about two of the small test blocks, far less than the requested range.
    private static final int CLIENT_STREAM_WINDOW_BYTES = 256;
    private static final long LAST_BLOCK_NUMBER = 19L;
    private static final Duration AWAIT_LIMIT = Duration.ofSeconds(30);
    /// HTTP/2 RST_STREAM error code STREAM_CLOSED.
    private static final long STREAM_CLOSED = 5L;
    // Block node production values, see WebServerHttp2Config, ServerConfig and ServiceBuilderImpl.
    private static final String HTTP2_PROTOCOL_CONFIG_NAME = "http_2";
    private static final int INITIAL_WINDOW_SIZE = 8_388_608;
    private static final int MAX_FRAME_SIZE = 8_388_608;
    private static final int MAX_MESSAGE_SIZE_BYTES = 131_072_000;
    private static final int WRITE_QUEUE_LENGTH = 8_192;

    private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
    private final WebServer webServer;

    SubscriberFlowControlStallTest() {
        super(new CompletionTrackingExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        final SubscriberServicePlugin plugin = new SubscriberServicePlugin();
        historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        start(plugin, plugin.methods().getFirst(), historicalBlockFacility);
        storeHistoricalBlocks(0L, 0L);
        webServer = startWebServer();
    }

    @AfterEach
    void stopWebServer() {
        webServer.stop();
    }

    @Test
    @DisplayName("A stall while sending historical blocks ends the call with exactly one trailers frame")
    void historicalStallEndsCallWithOneTrailers() throws Exception {
        storeHistoricalBlocks(1L, LAST_BLOCK_NUMBER);
        try (Http2FrameRecordingClient client = openStalledCall()) {
            awaitCallEnded(client);
            assertThat(client.endOfStreamHeaders()).hasSize(1);
        }
    }

    @Test
    @DisplayName("A stall while sending live blocks ends the call with exactly one trailers frame")
    void liveStallEndsCallWithOneTrailers() throws Exception {
        try (Http2FrameRecordingClient client = openStalledCall()) {
            // Block 0 comes from history, so the session is already registered for live blocks.
            client.awaitFirstData(AWAIT_LIMIT);
            publishLiveBlocks(1L, LAST_BLOCK_NUMBER);
            awaitCallEnded(client);
            assertThat(client.endOfStreamHeaders()).hasSize(1);
        }
    }

    @Test
    @DisplayName("A stall does not provoke the client into resetting a closed stream")
    void stallDoesNotProvokeStreamClosedReset() throws Exception {
        storeHistoricalBlocks(1L, LAST_BLOCK_NUMBER);
        try (Http2FrameRecordingClient client = openStalledCall()) {
            awaitCallEnded(client);
            assertThat(client.outboundResets()).noneMatch(reset -> reset.errorCode() == STREAM_CLOSED);
        }
    }

    @Test
    @DisplayName("A stall ends only the stalled call, not the connection")
    void stallDoesNotCloseConnection() throws Exception {
        storeHistoricalBlocks(1L, LAST_BLOCK_NUMBER);
        try (Http2FrameRecordingClient client = openStalledCall()) {
            awaitCallEnded(client);
            assertThat(client.inboundGoAwayErrorCodes()).isEmpty();
        }
    }

    @Test
    @DisplayName("A grpc-java client that stops reading past the flow-control timeout sees INTERNAL")
    void grpcJavaClientThatStopsReadingSeesInternal() throws Exception {
        storeHistoricalBlocks(1L, LAST_BLOCK_NUMBER);
        final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", webServer.port())
                .usePlaintext()
                .flowControlWindow(CLIENT_STREAM_WINDOW_BYTES)
                .build();
        try {
            final BlockingClientCall<byte[], byte[]> call = ClientCalls.blockingV2ServerStreamingCall(
                    channel,
                    subscribeMethodDescriptor(),
                    CallOptions.DEFAULT,
                    subscribeRequest().toByteArray());
            // A blocking call starts its stream on the first read. After that the client reads
            // nothing until the session has ended, like an importer busy processing a block.
            assertThat(call.read()).isNotNull();
            testThreadPoolManager.executor().awaitCompletedTasks(1, AWAIT_LIMIT);
            assertThat(readUntilCallEnds(call).getCode()).isEqualTo(Status.Code.INTERNAL);
        } finally {
            channel.shutdownNow().awaitTermination(AWAIT_LIMIT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /// Opens a call for blocks 0 to {@link #LAST_BLOCK_NUMBER} from a client that never reads.
    @NonNull
    private Http2FrameRecordingClient openStalledCall() throws InterruptedException {
        final Http2FrameRecordingClient client =
                new Http2FrameRecordingClient(webServer.port(), CLIENT_STREAM_WINDOW_BYTES);
        client.sendGrpcRequest(subscribePath(), subscribeRequest());
        return client;
    }

    /// Waits until the call has ended and every frame the server wrote for it has been recorded:
    /// the first trailers arrive, the session thread finishes, and a PING round trip flushes the
    /// connection behind anything the session wrote last.
    private void awaitCallEnded(@NonNull final Http2FrameRecordingClient client) throws Exception {
        client.awaitFirstEndOfStream(AWAIT_LIMIT);
        testThreadPoolManager.executor().awaitCompletedTasks(1, AWAIT_LIMIT);
        client.pingRoundTrip(AWAIT_LIMIT);
    }

    private void storeHistoricalBlocks(final long firstBlockNumber, final long lastBlockNumber) {
        for (long blockNumber = firstBlockNumber; blockNumber <= lastBlockNumber; blockNumber++) {
            historicalBlockFacility.handleBlockItemsReceived(
                    TestBlockBuilder.generateBlockWithNumber(blockNumber).asBlockItems());
        }
    }

    private void publishLiveBlocks(final long firstBlockNumber, final long lastBlockNumber) {
        for (long blockNumber = firstBlockNumber; blockNumber <= lastBlockNumber; blockNumber++) {
            blockMessaging.sendBlockItems(
                    TestBlockBuilder.generateBlockWithNumber(blockNumber).asBlockItems());
        }
    }

    @NonNull
    private static Status readUntilCallEnds(@NonNull final BlockingClientCall<byte[], byte[]> call)
            throws InterruptedException {
        try {
            while (call.read() != null) {
                // Drain the messages buffered before the call ended.
            }
            return Status.OK;
        } catch (final StatusException e) {
            return e.getStatus();
        }
    }

    @NonNull
    private WebServer startWebServer() {
        final Http2Config http2Config = Http2Config.builder()
                .name(HTTP2_PROTOCOL_CONFIG_NAME)
                .flowControlTimeout(FLOW_CONTROL_TIMEOUT)
                .initialWindowSize(INITIAL_WINDOW_SIZE)
                .maxFrameSize(MAX_FRAME_SIZE)
                .build();
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name("pbj")
                .maxMessageSizeBytes(MAX_MESSAGE_SIZE_BYTES)
                .build();
        return WebServerConfig.builder()
                .port(0)
                .addProtocol(http2Config)
                .addProtocol(pbjConfig)
                .writeQueueLength(WRITE_QUEUE_LENGTH)
                .addRouting(PbjRouting.builder().service(serviceInterface))
                .build()
                .start();
    }

    @NonNull
    private String subscribePath() {
        return "/" + serviceInterface.fullName() + "/" + method.name();
    }

    @NonNull
    private MethodDescriptor<byte[], byte[]> subscribeMethodDescriptor() {
        return MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName(serviceInterface.fullName() + "/" + method.name())
                .setRequestMarshaller(new ByteArrayMarshaller())
                .setResponseMarshaller(new ByteArrayMarshaller())
                .build();
    }

    @NonNull
    private static Bytes subscribeRequest() {
        return SubscribeStreamRequest.PROTOBUF.toBytes(SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0L)
                .endBlockNumber(LAST_BLOCK_NUMBER)
                .build());
    }

    /// Passes gRPC messages through as raw bytes.
    private static final class ByteArrayMarshaller implements MethodDescriptor.Marshaller<byte[]> {
        @NonNull
        @Override
        public InputStream stream(@NonNull final byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @NonNull
        @Override
        public byte[] parse(@NonNull final InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /// Runs each task on its own virtual thread, like the block node, and counts finished tasks so
    /// a test can wait for the subscriber session to end instead of sleeping.
    static final class CompletionTrackingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate = Executors.newVirtualThreadPerTaskExecutor();
        private final Semaphore completedTasks = new Semaphore(0);

        void awaitCompletedTasks(final int count, @NonNull final Duration timeout) throws InterruptedException {
            assertThat(completedTasks.tryAcquire(count, timeout.toMillis(), TimeUnit.MILLISECONDS))
                    .isTrue();
        }

        @Override
        public void execute(@NonNull final Runnable task) {
            delegate.execute(new CompletionCountingTask(task));
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @NonNull
        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(final long timeout, @NonNull final TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        private final class CompletionCountingTask implements Runnable {
            private final Runnable task;

            private CompletionCountingTask(@NonNull final Runnable task) {
                this.task = task;
            }

            @Override
            public void run() {
                try {
                    task.run();
                } finally {
                    completedTasks.release();
                }
            }
        }
    }
}
