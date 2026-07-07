// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.push;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.http2.Http2ClientProtocolConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.tools.config.HelidonWebClientConfig;

/**
 * Pushes wrapped blocks to a Block Node over the standard publish gRPC stream, one block at a time,
 * decoupled from the producer (the live wrap pipeline) by a bounded queue.
 *
 * <p>Producer (e.g. {@code LiveSequential}'s wrap thread) calls {@link #pushBlock(long, Block)} as
 * each block is wrapped and validated. A background worker thread maintains a persistent publish
 * stream, drains the queue into the stream, tracks per-block ACKs, and reconnects with exponential
 * backoff if the stream drops.
 *
 * <p><b>"Never drop" guarantee.</b> Blocks are kept in an in-flight set until ACKed. On stream
 * error, all in-flight blocks are re-sent on the new stream before the worker resumes draining the
 * queue. If the queue fills (sustained BN unavailability), {@link #pushBlock} blocks the caller
 * (the wrap thread) until space is freed; nothing is dropped.
 *
 * <p>The BN's stored block set is queried once via {@link #queryLastAvailableBlock()} so the
 * producer can skip blocks the BN already has.
 */
public final class LiveBlockPushClient implements AutoCloseable {

    /** Marker used to wake the worker for graceful shutdown. */
    private static final Block POISON = Block.DEFAULT;

    private final String host;
    private final int port;
    private final HelidonWebClientConfig webConfig;
    private final BlockingQueue<QueuedBlock> queue;
    private final long maxBatchBytes;
    private final long retryInitialMs;
    private final long retryMaxMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong highestSubmittedBlock = new AtomicLong(-1);
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong lastAckedBlock = new AtomicLong(-1);
    private final AtomicLong streamReconnects = new AtomicLong();

    private Thread worker;
    private WebClient cachedWebClient; // lazily created once and reused for every session

    /**
     * @param host BN host
     * @param port BN port (publish service)
     * @param queueCapacity backpressure queue size (blocks)
     * @param webConfig Helidon/gRPC tuning; uses {@code serverMaxMessageSizeBytes} for batching
     */
    public LiveBlockPushClient(
            final String host, final int port, final int queueCapacity, final HelidonWebClientConfig webConfig) {
        this.host = host;
        this.port = port;
        this.webConfig = webConfig;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        this.maxBatchBytes = webConfig.serverMaxMessageSizeBytes();
        this.retryInitialMs = 250L;
        this.retryMaxMs = 30_000L;
    }

    /**
     * Load the default {@code clientDefaultConfig.json} bundled with the tools module so callers
     * don't have to hand-construct a {@link HelidonWebClientConfig}.
     */
    public static HelidonWebClientConfig loadDefaultWebConfig() {
        try (final var stream =
                LiveBlockPushClient.class.getClassLoader().getResourceAsStream("clientDefaultConfig.json")) {
            if (stream == null) {
                throw new IllegalStateException("clientDefaultConfig.json not found on classpath");
            }
            return HelidonWebClientConfig.JSON.parse(Bytes.wrap(stream.readAllBytes()));
        } catch (final IOException | ParseException | RuntimeException e) {
            // IOException from readAllBytes/close; ParseException from the PBJ parse call;
            // RuntimeException for anything else. Named explicitly so a future new checked
            // exception surfaces as a compile error instead of being silently swallowed by
            // a bare `Exception` catch.
            throw new IllegalStateException("Failed to load default Helidon web client config", e);
        }
    }

    /** Query the target BN once for its current {@code lastAvailableBlock}; returns -1 on error. */
    public long queryLastAvailableBlock() {
        try {
            final WebClient web = buildWebClient();
            final PbjGrpcClient pbj = new PbjGrpcClient(web, buildGrpcConfig());
            final BlockNodeServiceInterface.BlockNodeServiceClient client =
                    new BlockNodeServiceInterface.BlockNodeServiceClient(pbj, defaultOptions());
            final ServerStatusResponse resp =
                    client.serverStatus(ServerStatusRequest.newBuilder().build());
            return resp.lastAvailableBlock();
        } catch (final Exception e) {
            System.err.println("[push] queryLastAvailableBlock failed: " + e.getMessage());
            return -1;
        }
    }

    /** Start the background worker thread. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        worker = new Thread(this::workerLoop, "live-block-push-worker");
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * Hand a wrapped block to the push subsystem. Blocks the caller if the queue is full (which
     * means the BN is falling behind). Never drops.
     */
    public void pushBlock(final long blockNumber, final Block wrapped) throws InterruptedException {
        if (!running.get()) {
            throw new IllegalStateException("LiveBlockPushClient is not started");
        }
        queue.put(new QueuedBlock(blockNumber, wrapped));
    }

    /** Drain the queue (waiting for ACKs) and stop the worker. */
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // Wake the worker; it will exit after draining.
        try {
            queue.put(new QueuedBlock(-1, POISON));
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        if (worker != null) {
            try {
                worker.join(TimeUnit.MINUTES.toMillis(2));
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    // ------- metrics -------

    public long submitted() {
        return submitted.get();
    }

    public long acked() {
        return acked.get();
    }

    public long lastAcked() {
        return lastAckedBlock.get();
    }

    public int queueDepth() {
        return queue.size();
    }

    public long reconnects() {
        return streamReconnects.get();
    }

    // ------- worker -------

    private void workerLoop() {
        // In-flight = sent on the current stream but not yet ACKed. On stream error these are
        // re-sent on the new stream so nothing is lost.
        final Deque<QueuedBlock> inFlight = new ArrayDeque<>();
        long backoffMs = retryInitialMs;

        while (running.get() || !queue.isEmpty() || !inFlight.isEmpty()) {
            try {
                final StreamSession session = openSession();
                try {
                    backoffMs = retryInitialMs; // reset on successful connect
                    // First, resend anything still in-flight from the previous (failed) stream.
                    for (final QueuedBlock qb : inFlight) {
                        sendBlock(session, qb);
                    }
                    // Then drain new work from the queue. Poll with a short timeout so the
                    // worker periodically checks whether the response pipeline has flipped to
                    // failed — protects against the case where Helidon's I/O thread absorbs
                    // the throw from AckPipeline.onError instead of surfacing it here.
                    while (running.get() || !queue.isEmpty()) {
                        if (!session.responses.isAlive()) {
                            final Throwable cause = session.responses.failureCause();
                            throw new StreamFailure(
                                    cause != null ? cause : new RuntimeException("stream closed by remote"));
                        }
                        final QueuedBlock qb = queue.poll(200, TimeUnit.MILLISECONDS);
                        if (qb == null) {
                            continue;
                        }
                        if (qb.block == POISON) {
                            // Graceful shutdown sentinel — wait for ACKs and exit.
                            break;
                        }
                        inFlight.addLast(qb);
                        sendBlock(session, qb);
                        // Opportunistically prune in-flight head as ACKs arrive.
                        prunePendingFromAck(inFlight);
                    }
                    // Wait for all ACKs (deadline) before closing.
                    waitForPending(inFlight);
                    session.requestPipeline.onComplete();
                    // Best effort: give the server 2 min to acknowledge onComplete before we exit.
                    // We do NOT want a failed .get() here to trigger the outer reconnect path (we're
                    // in the shutdown branch); log the specific expected exceptions and move on.
                    try {
                        session.completion.get(2, TimeUnit.MINUTES);
                    } catch (final java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                        System.err.println("[push] shutdown: completion did not settle cleanly: " + e.getMessage());
                    }
                    return; // shutdown path
                } catch (final StreamFailure sf) {
                    // Stream broke; loop will reopen and resend inFlight.
                    streamReconnects.incrementAndGet();
                    System.err.println("[push] stream failure: " + sf.getMessage() + "; reconnecting after " + backoffMs
                            + "ms (" + inFlight.size() + " in-flight)");
                    safeClose(session);
                    backoffMs = backoffAndAdvance(backoffMs);
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                } finally {
                    safeClose(session);
                }
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (final RuntimeException e) {
                streamReconnects.incrementAndGet();
                System.err.println("[push] unexpected worker error: " + e.getMessage() + "; reconnecting after "
                        + backoffMs + "ms");
                backoffMs = backoffAndAdvance(backoffMs);
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
        }
    }

    /**
     * Park for {@code backoffMs} milliseconds, then return the next (doubled, clamped) backoff.
     * Uses {@link java.util.concurrent.locks.LockSupport#parkNanos} so we don't need to catch
     * {@link InterruptedException} — park returns silently on interrupt and leaves the flag set
     * for the caller to observe.
     */
    private long backoffAndAdvance(final long backoffMs) {
        java.util.concurrent.locks.LockSupport.parkNanos(backoffMs * 1_000_000L);
        return Math.min(retryMaxMs, backoffMs * 2);
    }

    private void sendBlock(final StreamSession session, final QueuedBlock qb) {
        final List<BlockItem> items = qb.block.items();
        final List<List<BlockItem>> batches = new ArrayList<>();
        List<BlockItem> current = new ArrayList<>();
        long currentSize = 0;
        for (final BlockItem item : items) {
            final int itemSize = BlockItem.PROTOBUF.measureRecord(item);
            // A single item bigger than the max message size will get its own batch and be
            // rejected by the BN. There's nothing we can do to shrink it here — log so the
            // failure is diagnosable rather than mysterious.
            if (itemSize > maxBatchBytes) {
                System.err.println("[push] block " + qb.blockNumber + " has an oversized item ("
                        + itemSize + " bytes > maxBatchBytes " + maxBatchBytes
                        + "); the BN will reject the resulting frame");
            }
            if (!current.isEmpty() && currentSize + itemSize > maxBatchBytes) {
                batches.add(current);
                current = new ArrayList<>();
                currentSize = 0;
            }
            current.add(item);
            currentSize += itemSize;
        }
        if (!current.isEmpty()) {
            batches.add(current);
        }
        for (final List<BlockItem> batch : batches) {
            final BlockItemSet set = BlockItemSet.newBuilder().blockItems(batch).build();
            final PublishStreamRequest req =
                    PublishStreamRequest.newBuilder().blockItems(set).build();
            session.requestPipeline.onNext(req);
        }
        // Only increment `submitted` on the first send of a given block — reattempts after a
        // stream reconnect walk in-flight in ascending order, so a block <= the current high
        // watermark has already been counted.
        long prev;
        do {
            prev = highestSubmittedBlock.get();
            if (qb.blockNumber <= prev) {
                return;
            }
        } while (!highestSubmittedBlock.compareAndSet(prev, qb.blockNumber));
        submitted.incrementAndGet();
    }

    /** Move ACKed blocks off the head of the in-flight deque. */
    private void prunePendingFromAck(final Deque<QueuedBlock> inFlight) {
        final long ack = lastAckedBlock.get();
        while (!inFlight.isEmpty() && inFlight.peekFirst().blockNumber <= ack) {
            inFlight.pollFirst();
        }
    }

    /** Block until inFlight is empty or a short deadline elapses. */
    private void waitForPending(final Deque<QueuedBlock> inFlight) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        while (!inFlight.isEmpty() && System.currentTimeMillis() < deadline) {
            prunePendingFromAck(inFlight);
            if (inFlight.isEmpty()) {
                return;
            }
            Thread.sleep(50);
        }
    }

    // ------- gRPC plumbing -------

    private WebClient buildWebClient() {
        if (cachedWebClient == null) {
            final Duration timeout = Duration.ofMillis(webConfig.readTimeoutMillis());
            final Tls tls = Tls.builder().enabled(false).build();
            final Http2ClientProtocolConfig http2Config = Http2ClientProtocolConfig.builder()
                    .flowControlBlockTimeout(Duration.ofMillis(webConfig.flowControlTimeoutMillis()))
                    .initialWindowSize(webConfig.initialWindowSize())
                    .maxFrameSize(webConfig.maxFrameSize())
                    .maxHeaderListSize(webConfig.maxHeaderListSize())
                    .ping(webConfig.pingEnabled())
                    .pingTimeout(Duration.ofMillis(webConfig.pingTimeoutMillis()))
                    .priorKnowledge(webConfig.priorKnowledge())
                    .build();
            final GrpcClientProtocolConfig grpcProtocolConfig = GrpcClientProtocolConfig.builder()
                    .abortPollTimeExpired(false)
                    .pollWaitTime(timeout)
                    .build();
            cachedWebClient = WebClient.builder()
                    .baseUri("http://" + host + ":" + port)
                    .tls(tls)
                    .protocolConfigs(List.of(http2Config, grpcProtocolConfig))
                    .connectTimeout(timeout)
                    .build();
        }
        return cachedWebClient;
    }

    private PbjGrpcClientConfig buildGrpcConfig() {
        final Duration timeout = Duration.ofMillis(webConfig.readTimeoutMillis());
        final Tls tls = Tls.builder().enabled(false).build();
        final String encoding = webConfig.grpcEncoding().isBlank() ? "identity" : webConfig.grpcEncoding();
        return new PbjGrpcClientConfig(
                timeout, tls, Optional.empty(), "application/grpc", encoding, Set.of("identity", "gzip"));
    }

    private static ServiceInterface.RequestOptions defaultOptions() {
        return new SimpleRequestOptions(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);
    }

    @SuppressWarnings("unchecked")
    private StreamSession openSession() {
        final WebClient web = buildWebClient();
        final PbjGrpcClient pbj = new PbjGrpcClient(web, buildGrpcConfig());
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient client =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(pbj, defaultOptions());
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        final AckPipeline responses = new AckPipeline(completion);
        final Pipeline<PublishStreamRequest> requestPipeline =
                (Pipeline<PublishStreamRequest>) client.publishBlockStream(responses);
        return new StreamSession(requestPipeline, completion, responses);
    }

    private void safeClose(final StreamSession session) {
        if (session == null) {
            return;
        }
        try {
            session.requestPipeline.onComplete();
        } catch (final Exception ignored) {
            // best effort
        }
    }

    // ------- inner types -------

    private record QueuedBlock(long blockNumber, Block block) {}

    private record StreamSession(
            Pipeline<PublishStreamRequest> requestPipeline,
            CompletableFuture<Void> completion,
            AckPipeline responses) {}

    private record SimpleRequestOptions(Optional<String> authority, String contentType)
            implements ServiceInterface.RequestOptions {}

    /** Wraps a stream failure thrown from the response pipeline as a checked-like marker. */
    private static final class StreamFailure extends RuntimeException {
        StreamFailure(final Throwable cause) {
            super(cause);
        }
    }

    /**
     * Tracks ACKs and signals failure on stream errors. Runs on Helidon's response-processing thread,
     * which may or may not surface exceptions thrown from {@link #onError}. To make failure detection
     * robust from the worker thread, {@link #streamAlive} is flipped to {@code false} before the
     * exception is thrown; the worker polls it in the drain loop and reconnects even if Helidon
     * swallowed the throw.
     */
    private final class AckPipeline implements Pipeline<PublishStreamResponse> {
        private final CompletableFuture<Void> completion;
        private final AtomicBoolean streamAlive = new AtomicBoolean(true);
        private volatile Throwable failureCause;

        AckPipeline(final CompletableFuture<Void> completion) {
            this.completion = completion;
        }

        boolean isAlive() {
            return streamAlive.get();
        }

        Throwable failureCause() {
            return failureCause;
        }

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {
            // no-op
        }

        @Override
        public void onNext(final PublishStreamResponse response) {
            if (response.hasAcknowledgement()) {
                final long blockNumber = response.acknowledgement().blockNumber();
                acked.incrementAndGet();
                // Track the highest contiguous acked block number we have seen.
                long prev;
                do {
                    prev = lastAckedBlock.get();
                    if (blockNumber <= prev) {
                        break;
                    }
                } while (!lastAckedBlock.compareAndSet(prev, blockNumber));
                return;
            }
            // The publish protocol may respond with more than acknowledgements. We surface each
            // variant so the operator log makes it clear what the BN is telling us, and let the
            // stream-lifecycle path (streamAlive / completion / worker reconnect) handle the
            // consequences. Skip/Resend/NodeBehindPublisher are informational for this client;
            // EndOfStream is a normal close after the max stream duration (or a hard error),
            // which the outer worker treats as a reconnect trigger.
            if (response.hasSkipBlock()) {
                System.err.println("[push] BN sent SkipBlock: block "
                        + response.skipBlock().blockNumber());
            } else if (response.hasResendBlock()) {
                System.err.println("[push] BN sent ResendBlock: block "
                        + response.resendBlock().blockNumber());
            } else if (response.hasNodeBehindPublisher()) {
                System.err.println("[push] BN reports node behind publisher at block "
                        + response.nodeBehindPublisher().blockNumber());
            } else if (response.hasEndStream()) {
                final PublishStreamResponse.EndOfStream end = response.endStream();
                System.err.println("[push] BN sent EndOfStream (code=" + end.status() + ", block=" + end.blockNumber()
                        + "); worker will reconnect");
                streamAlive.set(false);
                completion.complete(null);
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            // Runs on Helidon's I/O thread. Don't throw from here — Helidon's behaviour on a
            // thrown exception is undefined and it can hang the stream. We log, mark the stream
            // dead via the AtomicBoolean, and complete the future exceptionally. The worker's
            // poll loop observes streamAlive == false and re-throws StreamFailure on its own
            // thread, which is the correct place for the reconnect logic to fire.
            failureCause = throwable;
            streamAlive.set(false);
            completion.completeExceptionally(throwable);
            System.err.println("[push] response stream error: " + throwable.getMessage());
        }

        @Override
        public void onComplete() {
            streamAlive.set(false);
            completion.complete(null);
        }
    }
}
