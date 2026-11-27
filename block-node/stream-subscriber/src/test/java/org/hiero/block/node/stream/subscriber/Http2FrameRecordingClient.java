// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/// A raw HTTP/2 client that sends one gRPC request and records the frames exchanged on the wire,
/// before Netty's stream state machine validates them, so a second trailers frame on a closed
/// stream is still seen. The client never returns flow-control window to the server, so it behaves
/// like a gRPC client whose application stopped reading: the server fills the initial window and
/// then blocks waiting for a WINDOW_UPDATE that never comes.
final class Http2FrameRecordingClient implements AutoCloseable {
    /// The gRPC compressed-flag byte plus the four byte message length.
    private static final int GRPC_MESSAGE_PREFIX_BYTES = 5;
    private static final long PING_PAYLOAD = 1L;

    /// One HEADERS frame read from the server.
    record InboundHeaders(
            int streamId, boolean endStream, @Nullable String grpcStatus) {}

    /// One RST_STREAM frame this client sent.
    record OutboundReset(int streamId, long errorCode) {}

    private final List<InboundHeaders> inboundHeaders = new CopyOnWriteArrayList<>();
    private final List<OutboundReset> outboundResets = new CopyOnWriteArrayList<>();
    private final List<Long> inboundGoAwayErrorCodes = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Void> firstData = new CompletableFuture<>();
    private final CompletableFuture<Void> firstEndOfStream = new CompletableFuture<>();
    private final CompletableFuture<Void> pingAcknowledged = new CompletableFuture<>();
    private final EventLoopGroup eventLoopGroup;
    private final Http2ConnectionHandler connectionHandler;
    private final Channel channel;

    /// Connects to the server and starts the HTTP/2 connection.
    ///
    /// @param port the server port on localhost
    /// @param initialStreamWindowBytes the client's SETTINGS_INITIAL_WINDOW_SIZE, the number of
    ///     bytes the server may send on the stream before it needs a WINDOW_UPDATE
    Http2FrameRecordingClient(final int port, final int initialStreamWindowBytes) throws InterruptedException {
        eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        connectionHandler = new Http2ConnectionHandlerBuilder()
                .server(false)
                .initialSettings(Http2Settings.defaultSettings().initialWindowSize(initialStreamWindowBytes))
                .frameListener(new WindowHoldingFrameListener())
                .frameLogger(new RecordingFrameLogger())
                .build();
        channel = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ClientInitializer())
                .connect(new InetSocketAddress("localhost", port))
                .sync()
                .channel();
    }

    /// Opens a stream and sends a gRPC request the way a server-streaming client does: HEADERS,
    /// then the request message in one DATA frame with END_STREAM.
    ///
    /// @param path the gRPC path, `/<service full name>/<method>`
    /// @param requestMessage the serialized request protobuf
    void sendGrpcRequest(@NonNull final String path, @NonNull final Bytes requestMessage) {
        final Http2Headers headers = new DefaultHttp2Headers()
                .method("POST")
                .scheme("http")
                .path(path)
                .authority("localhost")
                .set("content-type", "application/grpc+proto")
                .set("te", "trailers");
        final ByteBuf body = Unpooled.buffer(GRPC_MESSAGE_PREFIX_BYTES + (int) requestMessage.length());
        body.writeByte(0);
        body.writeInt((int) requestMessage.length());
        body.writeBytes(requestMessage.toByteArray());
        channel.eventLoop().submit(new RequestWriter(headers, body)).syncUninterruptibly();
    }

    /// Waits for the first DATA frame from the server.
    void awaitFirstData(@NonNull final Duration timeout) throws Exception {
        firstData.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /// Waits for the first HEADERS frame with END_STREAM, the first trailers of the call.
    void awaitFirstEndOfStream(@NonNull final Duration timeout) throws Exception {
        firstEndOfStream.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /// Sends a PING and waits for its ACK. The server writes a connection's frames in order, so
    /// once the ACK arrives every frame the server wrote before it has arrived and been recorded.
    void pingRoundTrip(@NonNull final Duration timeout) throws Exception {
        channel.eventLoop().submit(new PingWriter()).syncUninterruptibly();
        pingAcknowledged.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /// @return every HEADERS frame with END_STREAM read from the server, in arrival order
    @NonNull
    List<InboundHeaders> endOfStreamHeaders() {
        return inboundHeaders.stream().filter(InboundHeaders::endStream).toList();
    }

    /// @return every RST_STREAM frame this client sent, in order
    @NonNull
    List<OutboundReset> outboundResets() {
        return List.copyOf(outboundResets);
    }

    /// @return the error code of every GOAWAY frame read from the server, in order
    @NonNull
    List<Long> inboundGoAwayErrorCodes() {
        return List.copyOf(inboundGoAwayErrorCodes);
    }

    @Override
    public void close() {
        channel.close().syncUninterruptibly();
        eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private final class ClientInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(@NonNull final SocketChannel socketChannel) {
            socketChannel.pipeline().addLast(connectionHandler);
        }
    }

    /// Writes the request on the event loop, as Netty requires for encoder calls.
    private final class RequestWriter implements Runnable {
        private final Http2Headers headers;
        private final ByteBuf body;

        private RequestWriter(@NonNull final Http2Headers headers, @NonNull final ByteBuf body) {
            this.headers = headers;
            this.body = body;
        }

        @Override
        public void run() {
            final ChannelHandlerContext context = channel.pipeline().context(connectionHandler);
            final int streamId = connectionHandler.connection().local().incrementAndGetNextStreamId();
            connectionHandler.encoder().writeHeaders(context, streamId, headers, 0, false, context.newPromise());
            connectionHandler.encoder().writeData(context, streamId, body, 0, true, context.newPromise());
            context.flush();
        }
    }

    /// Writes a PING on the event loop.
    private final class PingWriter implements Runnable {
        @Override
        public void run() {
            final ChannelHandlerContext context = channel.pipeline().context(connectionHandler);
            connectionHandler.encoder().writePing(context, false, PING_PAYLOAD, context.newPromise());
            context.flush();
        }
    }

    /// Reports no DATA bytes as processed, so the client never sends a WINDOW_UPDATE, and
    /// completes the PING future when the ACK arrives.
    private final class WindowHoldingFrameListener extends Http2FrameAdapter {
        @Override
        public int onDataRead(
                final ChannelHandlerContext context,
                final int streamId,
                final ByteBuf data,
                final int padding,
                final boolean endOfStream) {
            return 0;
        }

        @Override
        public void onPingAckRead(final ChannelHandlerContext context, final long data) {
            pingAcknowledged.complete(null);
        }
    }

    /// Records frames as Netty reads and writes them, before any stream state validation.
    private final class RecordingFrameLogger extends Http2FrameLogger {
        private RecordingFrameLogger() {
            super(LogLevel.TRACE);
        }

        @Override
        public void logHeaders(
                final Direction direction,
                final ChannelHandlerContext context,
                final int streamId,
                final Http2Headers headers,
                final int padding,
                final boolean endStream) {
            recordHeaders(direction, streamId, headers, endStream);
        }

        @Override
        public void logHeaders(
                final Direction direction,
                final ChannelHandlerContext context,
                final int streamId,
                final Http2Headers headers,
                final int streamDependency,
                final short weight,
                final boolean exclusive,
                final int padding,
                final boolean endStream) {
            recordHeaders(direction, streamId, headers, endStream);
        }

        @Override
        public void logData(
                final Direction direction,
                final ChannelHandlerContext context,
                final int streamId,
                final ByteBuf data,
                final int padding,
                final boolean endStream) {
            if (direction == Direction.INBOUND) {
                firstData.complete(null);
            }
        }

        @Override
        public void logRstStream(
                final Direction direction,
                final ChannelHandlerContext context,
                final int streamId,
                final long errorCode) {
            if (direction == Direction.OUTBOUND) {
                outboundResets.add(new OutboundReset(streamId, errorCode));
            }
        }

        @Override
        public void logGoAway(
                final Direction direction,
                final ChannelHandlerContext context,
                final int lastStreamId,
                final long errorCode,
                final ByteBuf debugData) {
            if (direction == Direction.INBOUND) {
                inboundGoAwayErrorCodes.add(errorCode);
            }
        }

        private void recordHeaders(
                final Direction direction, final int streamId, final Http2Headers headers, final boolean endStream) {
            if (direction != Direction.INBOUND) {
                return;
            }
            final CharSequence grpcStatus = headers.get("grpc-status");
            inboundHeaders.add(
                    new InboundHeaders(streamId, endStream, grpcStatus == null ? null : grpcStatus.toString()));
            if (endStream) {
                firstEndOfStream.complete(null);
            }
        }
    }
}
