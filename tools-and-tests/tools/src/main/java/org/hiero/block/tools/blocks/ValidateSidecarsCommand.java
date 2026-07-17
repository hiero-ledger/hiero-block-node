// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.BlockSource;
import org.hiero.block.tools.blocks.validation.SidecarIntegrityValidation;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Sidecar-only wrapped-block validator. Streams a WRB archive block-by-block and confirms each
 * sidecar's SHA-384 hash appears in the record file's signed sidecar metadata list.
 *
 * <p>Unlike the general {@code validate} subcommand, this one runs only
 * {@link SidecarIntegrityValidation} — no hash-chain verification, no signature verification, no
 * HBAR supply / merkle tree / balance / state-file checks. It exists so operators can spot-check
 * mainnet / testnet / previewnet archives for sidecar tampering without the setup cost (address
 * book, state files, balance checkpoints) that the full pipeline requires.
 *
 * <p>Existing WRBs already carry both the raw sidecar bytes and the signed hash list at wrap
 * time, so this is a pure read-side check: no re-wrap is required, only run the command.
 *
 * <p>Example:
 * <pre>{@code
 * java -jar tools-all.jar blocks validate-sidecars /path/to/mainnet-archive
 * }</pre>
 */
@Command(
        name = "validate-sidecars",
        description = "Validates sidecar SHA-384 integrity for wrapped block streams (issue #3196)",
        mixinStandardHelpOptions = true)
public class ValidateSidecarsCommand implements Runnable {

    @SuppressWarnings("unused")
    @Parameters(index = "0..*", description = "Block files, directories, or zip archives to validate")
    private Path[] files;

    @Option(
            names = {"-v", "--verbose"},
            description = "Reserved for future use; every failure is now printed by default")
    private boolean verbose = false;

    @Option(
            names = {"--fail-fast"},
            description = "Stop at the first sidecar failure instead of continuing through the archive")
    private boolean failFast = false;

    @Option(
            names = {"--no-progress"},
            description = "Suppress the live progress bar (useful when redirecting output to a file)")
    private boolean noProgress = false;

    @Option(
            names = {"--threads"},
            description =
                    "Worker threads for parallel decompression + validation (default: available CPU cores - 1, minimum 1)")
    private int threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    @Option(
            names = {"--prefetch"},
            description = "Blocks to buffer ahead of the consumer (default: ${DEFAULT-VALUE})")
    private int prefetch = 512;

    @Override
    public void run() {
        if (files == null || files.length == 0) {
            System.err.println("Error: at least one block file, directory, or zip must be given");
            return;
        }

        final AtomicLong corruptZipCount = new AtomicLong(0);
        final List<BlockSource> discovered = BlockZipsUtilities.findBlockSources(files, corruptZipCount);
        if (discovered.isEmpty()) {
            System.err.println("Error: no block sources discovered in the given inputs");
            return;
        }

        System.out.println(Ansi.AUTO.string("@|yellow Sidecar integrity check:|@"));
        System.out.println("  Sources discovered: " + discovered.size());
        if (corruptZipCount.get() > 0) {
            System.out.println("  Corrupt zip archives skipped: " + corruptZipCount.get());
        }

        // Whole-zip sources are placeholders that need to be opened and expanded into per-entry
        // sources. Without this step, readBlockData would try to gunzip the .zip archive itself
        // and fail with "Not in GZIP format". Expansion also sorts by block number so the
        // progress bar becomes monotonic.
        System.out.println("  Expanding zip archives...");
        final List<BlockSource> sources = BlockZipsUtilities.expandWholeZipSources(discovered);
        final long totalBlocks = sources.size();
        System.out.println("  Blocks to check:    " + totalBlocks);
        System.out.println();

        System.out.println("  Worker threads:     " + threads);
        System.out.println("  Prefetch queue:     " + prefetch);
        System.out.println();

        final SidecarIntegrityValidation validation = new SidecarIntegrityValidation();
        final ProgressBar progress = new ProgressBar(totalBlocks, !noProgress);
        progress.start();

        // Producer thread streams sources; worker pool decompresses + validates in parallel; main
        // thread drains submission-order futures so progress + counters stay ordered. Bounded
        // queue provides backpressure so we don't OOM on a large archive.
        final ExecutorService workers = Executors.newFixedThreadPool(threads, r -> {
            final Thread t = new Thread(r, "sidecar-worker");
            t.setDaemon(true);
            return t;
        });
        final BlockingQueue<Future<BlockResult>> resultQueue = new ArrayBlockingQueue<>(Math.max(threads, prefetch));
        final AtomicLong stopRequested = new AtomicLong(0);

        final Thread producer = new Thread(
                () -> {
                    try {
                        for (final BlockSource source : sources) {
                            if (stopRequested.get() != 0) {
                                break;
                            }
                            final Future<BlockResult> f = workers.submit(() -> processOne(source, validation));
                            resultQueue.put(f);
                        }
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        // Sentinel: null-completed future so the consumer knows we're done.
                        try {
                            resultQueue.put(java.util.concurrent.CompletableFuture.completedFuture(null));
                        } catch (final InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                "sidecar-producer");
        producer.setDaemon(true);
        producer.start();

        long checked = 0;
        long failed = 0;
        long ioErrors = 0;
        final List<Long> failedBlocks = new ArrayList<>();
        String firstFailureMessage = null;

        while (true) {
            final Future<BlockResult> f;
            try {
                f = resultQueue.take();
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            final BlockResult r;
            try {
                r = f.get();
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (final ExecutionException ee) {
                // A worker threw something unexpected. Bump ioErrors and continue.
                ioErrors++;
                progress.advance();
                progress.pausePrintErr(Ansi.AUTO.string("@|red worker error:|@ "
                        + ee.getCause().getClass().getSimpleName() + ": "
                        + ee.getCause().getMessage()));
                continue;
            }
            if (r == null) {
                // Producer sentinel — all done.
                break;
            }
            if (r.ioError != null) {
                ioErrors++;
                progress.advance();
                progress.pausePrintErr(Ansi.AUTO.string("@|red I/O or parse error at block " + r.blockNumber + " ("
                        + r.filePath + "):|@ " + r.ioError));
                continue;
            }
            checked++;
            if (r.failureMessage != null) {
                failed++;
                failedBlocks.add(r.blockNumber);
                if (firstFailureMessage == null) {
                    firstFailureMessage = r.failureMessage;
                }
                // Every failure is printed by default; sidecar failures are rare enough that
                // suppressing them past the first hides exactly the information the investigator
                // needs (which blocks, and whether the failure mode is TAMPERED / MISSING / EXTRA).
                progress.pausePrintErr(Ansi.AUTO.string("@|red FAIL:|@ " + r.failureMessage));
                if (failFast) {
                    stopRequested.set(1);
                    progress.advance();
                    break;
                }
            }
            progress.advance();
        }
        progress.finish();
        workers.shutdownNow();
        try {
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow Summary:|@"));
        System.out.println("  Blocks checked: " + checked);
        System.out.println("  Blocks failed:  " + failed);
        if (ioErrors > 0) {
            System.out.println("  I/O or parse errors (uncounted): " + ioErrors);
        }
        if (!failedBlocks.isEmpty()) {
            System.out.println("  Failing block numbers: " + failedBlocks);
        }
        if (failed == 0 && ioErrors == 0) {
            System.out.println(Ansi.AUTO.string("@|green All sidecars verified successfully.|@"));
        } else if (failed > 0) {
            System.out.println(Ansi.AUTO.string(
                    "@|red " + failed + " block(s) failed sidecar integrity.|@ First failure: " + firstFailureMessage));
            System.exit(1);
        } else {
            System.out.println(Ansi.AUTO.string(
                    "@|yellow No sidecar failures observed, but " + ioErrors + " block(s) could not be read.|@"));
            System.exit(2);
        }
    }

    /**
     * Read + decompress + partial-parse + validate one block. Called on a worker thread.
     * Returns a {@link BlockResult} describing the outcome; never throws.
     */
    private static BlockResult processOne(final BlockSource source, final SidecarIntegrityValidation validation) {
        final BlockUnparsed block;
        try {
            final byte[][] data = BlockZipsUtilities.readBlockData(source);
            final boolean[] flags = BlockZipsUtilities.compressionFlags(source);
            // data[0] is the compressed bytes as read from disk / zip; that's what
            // decompressAndPartialParse expects together with the isZstd/isGz flags.
            block = BlockZipsUtilities.decompressAndPartialParse(data[0], flags[0], flags[1]);
        } catch (final IOException | RuntimeException e) {
            return BlockResult.io(source.blockNumber(), source.filePath().toString(), e.getMessage());
        } catch (final Exception e) {
            return BlockResult.io(source.blockNumber(), source.filePath().toString(), e.getMessage());
        }
        try {
            validation.validate(block, source.blockNumber());
            return BlockResult.ok(source.blockNumber());
        } catch (final ValidationException ve) {
            return BlockResult.failure(source.blockNumber(), ve.getMessage());
        }
    }

    /** Outcome of a single worker's block. Exactly one of ioError / failureMessage is non-null; both null == pass. */
    private record BlockResult(long blockNumber, String filePath, String ioError, String failureMessage) {
        static BlockResult ok(final long blockNumber) {
            return new BlockResult(blockNumber, null, null, null);
        }

        static BlockResult failure(final long blockNumber, final String message) {
            return new BlockResult(blockNumber, null, null, message);
        }

        static BlockResult io(final long blockNumber, final String filePath, final String message) {
            return new BlockResult(blockNumber, filePath, message, null);
        }
    }

    /**
     * Minimal single-thread block-count progress bar with a rate + ETA. Uses ANSI carriage return
     * so successive renders overwrite the same line; errors and diagnostic prints are routed
     * through {@link #pausePrintErr(String)} so they land on their own line above the bar.
     */
    private static final class ProgressBar {

        private static final long UPDATE_INTERVAL_MS = 200L;
        private static final int BAR_WIDTH = 30;

        private final long total;
        private final boolean enabled;

        private long startNanos;
        private long lastRenderMs;
        private long done;

        ProgressBar(final long total, final boolean enabled) {
            this.total = total;
            this.enabled = enabled && total > 0;
        }

        void start() {
            startNanos = System.nanoTime();
            lastRenderMs = 0;
            if (enabled) {
                render();
            }
        }

        void advance() {
            done++;
            if (!enabled) {
                return;
            }
            final long nowMs = elapsedMs();
            if (nowMs - lastRenderMs >= UPDATE_INTERVAL_MS || done == total) {
                render();
                lastRenderMs = nowMs;
            }
        }

        void finish() {
            if (!enabled) {
                return;
            }
            render();
            System.out.print("\n");
            System.out.flush();
        }

        void pausePrintErr(final String message) {
            if (enabled) {
                // Clear the current progress line before writing the diagnostic so both survive
                // untangled in a terminal.
                System.out.print("\r[K");
                System.out.flush();
            }
            System.err.println(message);
            if (enabled) {
                render();
            }
        }

        private long elapsedMs() {
            return (System.nanoTime() - startNanos) / 1_000_000L;
        }

        private void render() {
            final long elapsed = elapsedMs();
            final double fraction = total == 0 ? 0.0 : Math.min(1.0, done / (double) total);
            final int filled = (int) Math.round(fraction * BAR_WIDTH);
            final StringBuilder bar = new StringBuilder(BAR_WIDTH + 2);
            bar.append('[');
            for (int i = 0; i < BAR_WIDTH; i++) {
                bar.append(i < filled ? '#' : '.');
            }
            bar.append(']');

            final String rate = (elapsed > 0 && done > 0) ? formatRate(done, elapsed) : "--";
            final String eta = formatEta(done, total, elapsed);

            System.out.printf(
                    "\r%s %d/%d (%.1f%%) %s ETA %s", bar.toString(), done, total, fraction * 100.0, rate, eta);
            System.out.flush();
        }

        private static String formatRate(final long done, final long elapsedMs) {
            final double perSec = done / (elapsedMs / 1000.0);
            if (perSec >= 1000.0) {
                return String.format("%.1fk blk/s", perSec / 1000.0);
            }
            return String.format("%.0f blk/s", perSec);
        }

        private static String formatEta(final long done, final long total, final long elapsedMs) {
            if (done == 0 || elapsedMs == 0) {
                return "--";
            }
            if (done >= total) {
                return "0s";
            }
            final double remainingMs = (elapsedMs / (double) done) * (total - done);
            return formatDuration((long) remainingMs);
        }

        private static String formatDuration(final long ms) {
            long s = ms / 1000L;
            if (s < 60L) return s + "s";
            long m = s / 60L;
            s %= 60L;
            if (m < 60L) return m + "m" + s + "s";
            final long h = m / 60L;
            m %= 60L;
            return h + "h" + m + "m";
        }
    }
}
