// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.tools.commands.days.model.TarZstdDayReader;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.InMemoryBlock;
import org.hiero.block.tools.records.RecordFileInfo;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Validate blockchain in record file blocks in day files, computing and checking running hash.
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(name = "validate", description = "Validate blockchain in record file blocks in day files")
public class Validate implements Runnable {
    private static final Bytes ZERO_HASH = Bytes.wrap(new byte[48]);

    // Simple wrapper for producer-consumer communication (day boundaries + blocks)
    private static final class Item {
        enum Kind { DAY_START, BLOCK, DAY_END, STREAM_END }
        final Kind kind;
        final int dayIndex; // index into dayPaths
        final Path dayFile; // for diagnostics (may be null for STREAM_END)
        final InMemoryBlock block; // only for BLOCK
        Item(Kind kind, int dayIndex, Path dayFile, InMemoryBlock block) {
            this.kind = kind; this.dayIndex = dayIndex; this.dayFile = dayFile; this.block = block;
        }
        static Item dayStart(int idx, Path file) { return new Item(Kind.DAY_START, idx, file, null); }
        static Item block(int idx, Path file, InMemoryBlock b) { return new Item(Kind.BLOCK, idx, file, b); }
        static Item dayEnd(int idx, Path file) { return new Item(Kind.DAY_END, idx, file, null); }
        static Item streamEnd() { return new Item(Kind.STREAM_END, -1, null, null); }
    }

    @Spec
    CommandSpec spec;

    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] compressedDayOrDaysDirs = new File[0];

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDayOrDaysDirs.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        final AtomicReference<Bytes> carryOverHash = new AtomicReference<>(ZERO_HASH);
        System.out.println("Staring hash[" + carryOverHash.get() + "]");
        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(compressedDayOrDaysDirs);
        // Estimation/ETA support
        final long startNanos = System.nanoTime();
        final long INITIAL_ESTIMATE_PER_DAY = (24L * 60L * 60L) / 5L; // one every 5 seconds for a whole day
        final long dayCount = dayPaths.size();
        final AtomicLong totalProgress = new AtomicLong(dayCount * INITIAL_ESTIMATE_PER_DAY);
        final AtomicLong progress = new AtomicLong(0);

        // Producer-consumer queue with bounded capacity for backpressure
        final BlockingQueue<Item> queue = new LinkedBlockingQueue<>(100_000);

        // Reader thread: reads .tar.zstd day files and enqueues blocks
        final Thread reader = new Thread(() -> {
            try {
                for (int day = 0; day < dayPaths.size(); day++) {
                    final Path dayPath = dayPaths.get(day);
                    queue.put(Item.dayStart(day, dayPath));
                    try (var stream = TarZstdDayReader.streamTarZstd(dayPath)) {
                        final int dayIdx = day; // capture effectively final for lambda
                        // capture effectively final for lambda
                        stream.forEach(set -> {
                            try {
                                queue.put(Item.block(dayIdx, dayPath, set));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException("Interrupted while enqueueing block from " + dayPath, ie);
                            }
                        });
                    } catch (Exception ex) {
                        PrettyPrint.clearProgress();
                        System.err.println("Failed processing day file: " + dayPath + ": " + ex.getMessage());
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    // signal day end
                    queue.put(Item.dayEnd(day, dayPath));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                PrettyPrint.clearProgress();
                System.err.println("Reader thread interrupted");
                System.exit(1);
            } finally {
                try {
                    queue.put(Item.streamEnd());
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "validate-reader");
        reader.start();

        long progressAtStartOfDay = 0L;
        int currentDay = -1;

        try {
            // Consumer loop: validate blocks and update progress/ETA
            while (true) {
                final Item item = queue.take();
                if (item.kind == Item.Kind.STREAM_END) break;

                switch (item.kind) {
                    case DAY_START -> {
                        currentDay = item.dayIndex;
                        progressAtStartOfDay = progress.get();
                    }
                    case BLOCK -> {
                        final InMemoryBlock set = item.block;
                        // update counters
                        progress.incrementAndGet();
                        try {
                            final RecordFileInfo recordFileInfo =
                                RecordFileInfo.parse(set.primaryRecordFile().data());

                            // previous block hash from prior iteration (carry over)
                            final Bytes previousBlockHash = carryOverHash.get();
                            // update carry over to current block hash for next iteration
                            carryOverHash.set(recordFileInfo.blockHash());

                            // Validate linkage: previous hash must equal carry-over from the last block
                            if (!recordFileInfo.previousBlockHash().equals(previousBlockHash)) {
                                PrettyPrint.clearProgress();
                                System.err.println(
                                    "Validation failed for " + set.recordFileTime() + " - expected prev="
                                        + previousBlockHash + " but found prev=" + recordFileInfo.previousBlockHash());
                                System.out.flush();
                                System.exit(1);
                            }

                            // Build progress string showing time and hashes (shortened to 8 chars for readability)
                            final String progressString = String.format(
                                "%32s carry[%s] prev[%s] next[%s]",
                                set.recordFileTime(),
                                shortHash(previousBlockHash),
                                shortHash(recordFileInfo.previousBlockHash()),
                                shortHash(recordFileInfo.blockHash()));

                            // Estimate totals and ETA
                            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;

                            // Progress percent and remaining
                            final long processedSoFarAcrossAll = progress.get();
                            final long totalProgressFinal = totalProgress.get();
                            double percent = ((double) processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;

                            long remainingMillis;
                            if (processedSoFarAcrossAll > 0) {
                                long remainingUnits = totalProgressFinal - processedSoFarAcrossAll;
                                remainingMillis = (long) ((elapsedMillis * (double) remainingUnits)
                                    / (double) processedSoFarAcrossAll);
                                if (remainingMillis < 0) {
                                    remainingMillis = 0;
                                }
                            } else {
                                remainingMillis = Long.MAX_VALUE; // unknown ETA at the very start
                            }

                            PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                        } catch (Exception ex) {
                            PrettyPrint.clearProgress();
                            System.err.println("Validation threw for " + set.recordFileTime() + ": " + ex.getMessage());
                            ex.printStackTrace();
                            System.exit(1);
                        }
                    }
                    case DAY_END -> {
                        // After finishing the day, update aggregates
                        final long progressAtEndOfDay = progress.get();
                        final long blocksInDay = progressAtEndOfDay - progressAtStartOfDay;
                        final long remainingDays = dayCount - (currentDay + 1);
                        // update total estimate based on actual blocks processed in this day
                        totalProgress.set(progressAtEndOfDay + (remainingDays * blocksInDay));
                    }
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            PrettyPrint.clearProgress();
            System.err.println("Validation interrupted");
            System.exit(1);
        }

        // clear the progress line once done and print a summary
        PrettyPrint.clearProgress();
        System.out.println("Validation complete. Days processed: " + dayCount + " , Blocks processed: " + progress.get());

        // Ensure reader finished
        try { reader.join(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    /**
     * Shorten a hash to prefix 8 chars of hex for readability
     *
     * @param hash the hash to shorten
     * @return the shortened hash
     */
    private static String shortHash(final Bytes hash) {
        if (hash == null) return "null";
        final String s = hash.toString();
        return s.length() <= 8 ? s : s.substring(0, 8);
    }
}
