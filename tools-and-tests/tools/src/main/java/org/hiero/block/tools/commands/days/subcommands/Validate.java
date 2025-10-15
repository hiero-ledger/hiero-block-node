// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
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

        for (int day = 0; day < dayPaths.size(); day++) {
            Path dayFile = dayPaths.get(day);
            try (var stream = TarZstdDayReader.streamTarZstd(dayFile)) {
                final long progressAtStartOfDay = progress.get();
                stream.forEach((InMemoryBlock set) -> {
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
                            "%s carry[%s] prev[%s] next[%s]",
                            set.recordFileTime(),
                            shortHash(previousBlockHash),
                            shortHash(recordFileInfo.previousBlockHash()),
                            shortHash(recordFileInfo.blockHash()));

                        // Estimate totals and ETA
                        final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;

                        // Progress percent and remaining
                        final long processedSoFarAcrossAll = progress.get();
                        final long totalProgressFinal = totalProgress.get();
                        double percent = ((double)processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;

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
                });

                // After finishing the day, update aggregates
                final long progressAtEndOfDay = progress.get();
                final long blocksInDay = progressAtEndOfDay - progressAtStartOfDay;
                final long remainingDays = dayCount - (day + 1);
                // update total estimate based on actual blocks processed in this day
                totalProgress.set(progressAtEndOfDay + (remainingDays * blocksInDay));
            } catch (Exception ex) {
                PrettyPrint.clearProgress();
                System.err.println("Failed processing day file: " + dayFile + ": " + ex.getMessage());
                ex.printStackTrace();
                System.exit(1);
            }
        }
        // clear the progress line once done and print a summary
        PrettyPrint.clearProgress();
        System.out.println("Validation complete. Days processed: "+dayCount+" , Blocks processed: " + progress.get());
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
