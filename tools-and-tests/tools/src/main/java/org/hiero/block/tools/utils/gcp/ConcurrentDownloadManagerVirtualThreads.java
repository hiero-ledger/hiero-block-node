package org.hiero.block.tools.utils.gcp;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.hiero.block.tools.commands.days.download.DownloadConstants;
import org.hiero.block.tools.records.InMemoryFile;

/**
 * ConcurrentDownloadManager (Java 21, virtual threads)
 * ----------------------------------------------------
 * High-throughput concurrent downloader for Google Cloud Storage (GCS) objects
 * from hosts OUTSIDE GCP, tuned for billions of small files (1 KB – 3 MB).
 *
 * Design overview:
 *  - Uses Java 21 virtual threads (Loom) for lightweight, massive concurrency.
 *    Concurrency is still bounded by an adaptive gate (AIMD control) so we
 *    never exceed what GCS (or your network) will tolerate.
 *  - Per-object exponential backoff with full jitter following GCS guidance:
 *      https://cloud.google.com/storage/docs/request-rate
 *      https://cloud.google.com/storage/docs/retry-strategy
 *  - Detects common rate-limit/server/network conditions (HTTP 408/429/5xx,
 *    SocketTimeoutException, IOExceptions) — including patterns like
 *    RetryBudgetExhausted/Read timed out observed in google-cloud-storage.
 *
 * Key differences from a naive thread-pool approach:
 *  - Virtual threads avoid the kernel-thread bottleneck and context switching.
 *  - AIMD (additive increase / multiplicative decrease) quickly ramps up
 *    concurrency when the error rate is low, and backs off immediately on
 *    rate-limiting, stabilizing near the service/network limit.
 *  - A sliding error window feeds the controller with a short-term view of
 *    retriable errors to decide cuts and cooldowns.
 *
 * Dependencies:
 *  - com.google.cloud:google-cloud-storage:2.x
 *
 * Suggested Storage client settings:
 *  - Prefer handing retries here in this class for GET operations. Configure
 *    the Storage client with low/one attempt so we control timing precisely:
 *
 *      Storage storage = StorageOptions.newBuilder()
 *          .setRetrySettings(RetrySettings.newBuilder()
 *              .setMaxAttempts(1)              // let this class do retries
 *              .setTotalTimeout(Duration.ofHours(1))
 *              .build())
 *          .build()
 *          .getService();
 *
 * Example usage (batching & in-order processing):
 *
 *   var mgr = ConcurrentDownloadManager.newBuilder(storage)
 *       .setInitialConcurrency(512)
 *       .setMaxConcurrency(8192)
 *       .setRampUpStep(128)
 *       .build();
 *
 *   try (mgr) {
 *     List<BlobId> batch =  List.of(); // next batch
 *     // Create futures in input order:
 *     List<CompletableFuture<byte[]>> futures = new ArrayList<>(batch.size());
 *     for (BlobId id : batch) {
 *       futures.add(mgr.downloadAsync(id.getBucket(), id.getName()));
 *     }
 *     // Wait for all to complete:
 *     CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
 *     // Process results IN ORDER:
 *     for (int i = 0; i < batch.size(); i++) {
 *       byte[] data = futures.get(i).join(); // preserves input order
 *       // ...convert/process/write later...
 *     }
 *   }
 */
public final class ConcurrentDownloadManagerVirtualThreads implements
    ConcurrentDownloadManager {
    /** GCP BlobSourceOption to use userProject for billing */
    public static final Storage.BlobSourceOption BLOB_SOURCE_OPTION =
        Storage.BlobSourceOption.userProject(DownloadConstants.GCP_PROJECT_ID);

    /** Google Cloud Storage client. All calls are idempotent GETs. */
    private final Storage storage;

    /**
     * Execution service backed by Java 21 virtual threads (unbounded). The actual concurrency
     * is bounded by {@link #gate} (AIMD controller). We choose virtual threads to cheaply park
     * during backoff and I/O without tying up carrier kernel threads.
     */
    private final ExecutorService exec;

    /** Small, platform-thread scheduled executor for periodic ramp-up and window ticks. */
    private final ScheduledExecutorService scheduler;

    /** Adaptive gate controls number of in-flight tasks (AIMD). */
    private final AdaptiveGate gate;

    /** Per-object retry/backoff policy. */
    private final RetryPolicy retryPolicy;

    /** Additive-increase timer period. */
    private final Duration rampUpInterval;

    /** Additive increment applied each {@link #rampUpInterval}. */
    private final int rampUpStep;

    /** Error-rate threshold (in sliding window) above which to cut concurrency. */
    private final double errorRateForCut;

    /** Multiplicative decrease factor applied when cutting concurrency. */
    private final double cutMultiplier;

    /** Cooldown duration after a cut to avoid immediate re-expansion. */
    private final Duration globalCooldown;

    /** Upper bound for concurrency. */
    private final int maxConcurrency;

    /** Cumulative bytes downloaded (best-effort; updated after each object). */
    private final AtomicLong bytesDownloaded = new AtomicLong();

    /** Cumulative completed objects. */
    private final AtomicLong objectsCompleted = new AtomicLong();

    /** Sliding error window for recent retriable error rate and cooldown gate. */
    private final ErrorWindow errorWindow;

    /** Flag to prevent scheduling when closed. */
    private volatile boolean closed = false;

    /** Private constructor from builder. */
    private ConcurrentDownloadManagerVirtualThreads(Builder b) {
        this.storage = Objects.requireNonNull(b.storage, "storage");
        this.maxConcurrency = b.maxConcurrency;

        // Virtual-thread per task executor (Java 21).
        this.exec = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name(b.threadNamePrefix + "-", 1).factory());

        // Small scheduled executor on platform threads.
        this.scheduler = Executors.newScheduledThreadPool(
            2, r -> {
                Thread t = new Thread(r, b.threadNamePrefix + "-sched");
                t.setDaemon(true);
                return t;
            });

        this.gate = new AdaptiveGate(b.initialConcurrency, b.minConcurrency, b.maxConcurrency);
        this.retryPolicy = new RetryPolicy(
            b.maxRetryAttempts, b.initialBackoff, b.maxBackoff, ConcurrentDownloadManagerVirtualThreads::defaultRetriable);
        this.rampUpInterval = b.rampUpInterval;
        this.rampUpStep = b.rampUpStep;
        this.errorRateForCut = b.errorRateForCut;
        this.cutMultiplier = b.cutMultiplier;
        this.globalCooldown = b.globalCooldown;
        this.errorWindow = new ErrorWindow(b.errorWindowSeconds);

        // Periodic additive increase (AIMD) — disabled during cooldown.
        scheduler.scheduleAtFixedRate(() -> {
            if (closed) return;
            if (errorWindow.inCooldownUntil > System.nanoTime()) return;
            gate.increase(rampUpStep);
        }, rampUpInterval.toMillis(), rampUpInterval.toMillis(), TimeUnit.MILLISECONDS);

        // Periodic error-window decay.
        scheduler.scheduleAtFixedRate(errorWindow::tick, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Create a new {@link Builder}.
     * @param storage configured {@link Storage} client. Prefer setting {@code maxAttempts=1}
     *                in {@link RetrySettings} so that this manager fully controls backoff.
     */
    public static Builder newBuilder(Storage storage) { return new Builder(storage); }

    // =====================
    // High-level APIs
    // =====================

    /**
     * Asynchronously download a small object fully into memory.
     *
     * <p>This is the primary API for small-object flows where the caller needs to stage
     * the bytes for ordered, batched processing before writing transformed output.</p>
     *
     * <p>Concurrency is automatically bounded by the AIMD gate; the returned
     * {@link CompletableFuture} completes when the object has been downloaded or
     * completes exceptionally on a non-retriable or exhausted-retries error.</p>
     *
     * <pre>{@code
     * List<BlobId> batch = ...;
     * List<CompletableFuture<InMemoryFile>> futures = new ArrayList<>();
     * for (BlobId id : batch) {
     *   futures.add(manager.downloadAsync(id.getBucket(), id.getName()));
     * }
     * CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
     * // Now process in-order:
     * for (int i = 0; i < batch.size(); i++) {
     *   InMemoryFile data = futures.get(i).join();
     *   // process data...
     * }
     * }</pre>
     *
     * @param bucketName GCS bucket
     * @param objectName GCS object name
     * @return future completing with object bytes
     * @throws IllegalStateException if the manager is closed
     */
    @SuppressWarnings("unused")
    public CompletableFuture<InMemoryFile> downloadAsync(String bucketName, String objectName) {
        if (closed) throw new IllegalStateException("Manager is closed");
        return CompletableFuture.supplyAsync(() -> {
            gate.acquire();
            try {
                return downloadWithRetryBytes(bucketName, objectName);
            } finally {
                gate.release();
            }
        }, exec);
    }

    /** Close immediately, cancelling outstanding work and stopping background schedulers. */
    @Override public void close() {
        closed = true;
        exec.shutdownNow();
        scheduler.shutdownNow();
    }

    // ====== Helpers for progress ======

    /** @return bytes downloaded so far (best-effort) */
    public long getBytesDownloaded() { return bytesDownloaded.get(); }

    /** @return objects completed so far */
    public long getObjectsCompleted() { return objectsCompleted.get(); }

    /** @return current concurrency limit */
    public int getCurrentConcurrency() { return gate.getLimit(); }

    /** @return max concurrency configured */
    public int getMaxConcurrency() { return maxConcurrency; }

    // ===== Core download with retry/backoff =====

    /** Bytes variant for {@link #downloadAsync(String, String)}. */
    private InMemoryFile downloadWithRetryBytes(String bucketName, String objectName) {
        int attempt = 0;
        long nextBackoffMs = retryPolicy.initialBackoff.toMillis();
        Random jitter = ThreadLocalRandom.current();

        while (true) {
            attempt++;
            try {
                byte[] data = storage.readAllBytes(bucketName, objectName, BLOB_SOURCE_OPTION);
                bytesDownloaded.addAndGet(data.length);
                objectsCompleted.incrementAndGet();
                return new InMemoryFile(Path.of(objectName), data);
            } catch (Throwable t) {
                boolean retriable = retryPolicy.isRetriable(t);
                errorWindow.recordError(retriable);
                if (!retriable || attempt >= retryPolicy.maxAttempts) {
                    maybeCutConcurrency(t);
                    if (t instanceof RuntimeException re) throw re;
                    throw new CompletionException(t);
                }
                long sleepMs = (long) (jitter.nextDouble(0.0, 1.0) * nextBackoffMs);
                quietlySleep(sleepMs);
                nextBackoffMs = Math.min(nextBackoffMs * 2, retryPolicy.maxBackoff.toMillis());
                maybeCutConcurrency(t);
            }
        }
    }

    /** AIMD cut logic shared across both variants. */
    private void maybeCutConcurrency(Throwable t) {
        boolean shouldCut = isRateLimitedOrServerError(t) ||
            errorWindow.currentErrorRate() >= errorRateForCut;
        if (!shouldCut) return;
        int before = gate.getLimit();
        int after = Math.max(gate.minLimit, (int) Math.floor(before * cutMultiplier));
        if (after < before) {
            gate.setLimit(after);
            errorWindow.startCooldown(globalCooldown);
        }
    }

    /**
     * Detect canonical rate-limit/server/network conditions that warrant backoff.
     * Incorporates patterns observed in google-cloud-storage stack traces such as:
     * StorageException(... "storage.googleapis.com" ...), SocketTimeoutException: Read timed out,
     * RetryBudgetExhaustedComment / BackoffComment.
     */
    private static boolean isRateLimitedOrServerError(Throwable t) {
        StorageException se = rootStorageException(t);
        if (se != null) {
            int code = se.getCode();
            if (code == 429 || code == 408) return true;
            if (code >= 500 && code <= 599) return true;
            if (se.getCause() instanceof SocketTimeoutException) return true;
            if (se.getMessage() != null) {
                String m = se.getMessage().toLowerCase();
                if (m.contains("retrybudgetexhausted") || m.contains("read timed out") ||
                    m.contains("deadline") || m.contains("backoff")) {
                    return true;
                }
            }
        }
        if (t instanceof SocketTimeoutException) return true;
        if (t instanceof IOException) return true;
        if (t.getCause() != null) return isRateLimitedOrServerError(t.getCause());
        return false;
    }

    /** Unwrap the first {@link StorageException} in a causal chain, or {@code null}. */
    private static StorageException rootStorageException(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof StorageException) return (StorageException) cur;
            cur = cur.getCause();
        }
        return null;
    }

    private static void quietlySleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    // ===== Support types =====

    /** Builder for {@link ConcurrentDownloadManagerVirtualThreads}. */
    @SuppressWarnings("unused")
    public static final class Builder {
        private final Storage storage;
        private int initialConcurrency = 64;
        private int minConcurrency = 8;
        private int maxConcurrency = 2048;
        private Duration rampUpInterval = Duration.ofSeconds(2);
        private int rampUpStep = 16;
        private double errorRateForCut = 0.02;
        private double cutMultiplier = 0.5;
        private Duration globalCooldown = Duration.ofSeconds(10);
        private int maxRetryAttempts = 12;
        private Duration initialBackoff = Duration.ofMillis(250);
        private Duration maxBackoff = Duration.ofSeconds(60);
        private int errorWindowSeconds = 30;
        private String threadNamePrefix = "gcs-dl";

        public Builder(Storage storage) { this.storage = storage; }
        public Builder setInitialConcurrency(int v) { this.initialConcurrency = v; return this; }
        public Builder setMinConcurrency(int v) { this.minConcurrency = v; return this; }
        public Builder setMaxConcurrency(int v) { this.maxConcurrency = v; return this; }
        public Builder setRampUpInterval(Duration v) { this.rampUpInterval = v; return this; }
        public Builder setRampUpStep(int v) { this.rampUpStep = v; return this; }
        public Builder setErrorRateForCut(double v) { this.errorRateForCut = v; return this; }
        public Builder setCutMultiplier(double v) { this.cutMultiplier = v; return this; }
        public Builder setGlobalCooldown(Duration v) { this.globalCooldown = v; return this; }
        public Builder setMaxRetryAttempts(int v) { this.maxRetryAttempts = v; return this; }
        public Builder setInitialBackoff(Duration v) { this.initialBackoff = v; return this; }
        public Builder setMaxBackoff(Duration v) { this.maxBackoff = v; return this; }
        public Builder setErrorWindowSeconds(int v) { this.errorWindowSeconds = v; return this; }
        public Builder setThreadNamePrefix(String v) { this.threadNamePrefix = v; return this; }
        public ConcurrentDownloadManagerVirtualThreads build() { return new ConcurrentDownloadManagerVirtualThreads(this); }
    }

    /** Default retriable predicate aligned with GCS guidance and observed exception patterns. */
    private static boolean defaultRetriable(Throwable t) {
        StorageException se = rootStorageException(t);
        if (se != null) {
            int code = se.getCode();
            if (code == 408 || code == 429) return true;
            if (code >= 500 && code <= 599) return true;
            if (se.getCause() instanceof SocketTimeoutException) return true;
            String m = se.getMessage();
            if (m != null) {
                String s = m.toLowerCase();
                if (s.contains("retrybudgetexhausted") || s.contains("read timed out") ||
                    s.contains("deadline") || s.contains("backoff")) {
                    return true;
                }
            }
        }
        if (t instanceof SocketTimeoutException) return true;
        if (t instanceof IOException) return true;
        Throwable c = t.getCause();
        return c != null && defaultRetriable(c);
    }

    /** Simple container for retry/backoff knobs and predicate. */
    private static final class RetryPolicy {
        final int maxAttempts;
        final Duration initialBackoff;
        final Duration maxBackoff;
        final Predicate<Throwable> retriable;
        RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff, Predicate<Throwable> retriable) {
            this.maxAttempts = maxAttempts;
            this.initialBackoff = initialBackoff;
            this.maxBackoff = maxBackoff;
            this.retriable = retriable;
        }
        boolean isRetriable(Throwable t) { return retriable.test(t); }
    }

    /** Adaptive concurrency gate with AIMD control. */
    private static final class AdaptiveGate {
        private final Object lock = new Object();
        private final int minLimit;
        private final int maxLimit;
        private int limit;
        private int inFlight = 0;

        AdaptiveGate(int initial, int min, int max) {
            this.limit = Math.max(min, Math.min(max, initial));
            this.minLimit = Math.max(1, min);
            this.maxLimit = Math.max(this.minLimit, max);
        }

        void acquire() {
            synchronized (lock) {
                while (inFlight >= limit) {
                    try { lock.wait(); } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted acquiring gate", e);
                    }
                }
                inFlight++;
            }
        }
        void release() {
            synchronized (lock) {
                inFlight--;
                lock.notifyAll();
            }
        }
        void increase(int step) {
            synchronized (lock) {
                int before = limit;
                limit = Math.min(maxLimit, limit + Math.max(1, step));
                if (limit != before) lock.notifyAll();
            }
        }
        void setLimit(int newLimit) {
            synchronized (lock) {
                int clamped = Math.max(minLimit, Math.min(maxLimit, newLimit));
                limit = clamped;
                lock.notifyAll();
            }
        }
        int getLimit() { synchronized (lock) { return limit; } }
    }

    /** Sliding-window error tracker with cooldown switch. */
    private static final class ErrorWindow {
        private final int seconds;
        private final AtomicInteger[] retriableErrors;
        private final AtomicInteger[] totals;
        private volatile int cursor = 0;
        private final Object tickLock = new Object();
        private volatile long inCooldownUntil = 0L; // nanoTime

        ErrorWindow(int seconds) {
            this.seconds = Math.max(5, seconds);
            this.retriableErrors = new AtomicInteger[this.seconds];
            this.totals = new AtomicInteger[this.seconds];
            for (int i = 0; i < this.seconds; i++) {
                retriableErrors[i] = new AtomicInteger();
                totals[i] = new AtomicInteger();
            }
        }

        void recordError(boolean retriable) {
            int idx = cursor;
            totals[idx].incrementAndGet();
            if (retriable) retriableErrors[idx].incrementAndGet();
        }

        void tick() {
            synchronized (tickLock) {
                cursor = (cursor + 1) % seconds;
                retriableErrors[cursor].set(0);
                totals[cursor].set(0);
            }
        }

        double currentErrorRate() {
            long e = 0, t = 0;
            for (int i = 0; i < seconds; i++) {
                e += retriableErrors[i].get();
                t += totals[i].get();
            }
            if (t == 0) return 0.0;
            return (double) e / (double) t;
        }

        void startCooldown(Duration d) {
            inCooldownUntil = System.nanoTime() + d.toNanos();
        }
    }
}
