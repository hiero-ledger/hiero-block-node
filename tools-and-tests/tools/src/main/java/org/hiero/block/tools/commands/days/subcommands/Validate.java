// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;
import org.hiero.block.tools.commands.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.InMemoryBlock;
import org.hiero.block.tools.records.InMemoryBlock.ValidationResult;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

/**
 * Validate blockchain in record file blocks in day files, computing and checking running hash.
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(name = "validate", description = "Validate blockchain in record file blocks in day files")
public class Validate implements Runnable {
    /** Zero hash for initial carry over */
    private static final byte[] ZERO_HASH = new byte[48];

    /** Gson instance for Status JSON serialization */
    private static final Gson GSON = new GsonBuilder().create();

    /**
     * Simple wrapper for producer-consumer communication (day boundaries + blocks)
     *
     * @param dayIndex index into dayPaths
     * @param dayFile  for diagnostics (may be null for STREAM_END)
     * @param block    only for BLOCK
     */
     private record Item(Kind kind, int dayIndex, Path dayFile, InMemoryBlock block) {
        enum Kind {
            DAY_START,
            BLOCK,
            DAY_END,
            STREAM_END
        }

        static Item dayStart(int idx, Path file) {
            return new Item(Kind.DAY_START, idx, file, null);
        }

        static Item block(int idx, Path file, InMemoryBlock b) {
            return new Item(Kind.BLOCK, idx, file, b);
        }

        static Item dayEnd(int idx, Path file) {
            return new Item(Kind.DAY_END, idx, file, null);
        }

        static Item streamEnd() {
            return new Item(Kind.STREAM_END, -1, null, null);
        }
    }

    /**
     * Simple status object saved to compressedDaysDir/validateCmdStatus.json to allow resuming.
     * <p>Note: JSON (de)serialization handled by Gson via field reflection.</p>
     */
    @SuppressWarnings("ClassCanBeRecord")
    private static final class Status {
        final int dayIndex;
        final String recordFileTime; // ISO-8601 from Instant.toString()
        final String endRunningHashHex;

        Status(int dayIndex, String recordFileTime, String endRunningHashHex) {
            this.dayIndex = dayIndex;
            this.recordFileTime = recordFileTime;
            this.endRunningHashHex = endRunningHashHex;
        }

        Instant recordInstant() {
            return Instant.parse(recordFileTime);
        }

        byte[] hashBytes() {
            return HexFormat.of().parseHex(endRunningHashHex);
        }

        private static void writeStatusFile(Path statusFile, Status s) {
            if (statusFile == null || s == null) return;
            try {
                String json = GSON.toJson(s);
                Files.writeString(statusFile, json, StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING);
            } catch (IOException e) {
                System.err.println("Failed to write status file " + statusFile + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        private static Status readStatusFile(Path statusFile) {
            try {
                if (statusFile == null || !Files.exists(statusFile)) return null;
                String content = Files.readString(statusFile, StandardCharsets.UTF_8);
                return GSON.fromJson(content, Status.class);
            } catch (Exception e) {
                System.err.println("Failed to read/parse status file " + statusFile + ": " + e.getMessage());
                return null;
            }
        }
    }

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-w", "--warnings-file"},
            description = "Write warnings to this file, rather than ignoring them")
    private File warningFile = null;

    @Parameters(index = "0", description = "Directories of days to process")
    @SuppressWarnings("unused") // assigned reflectively by picocli
    private File compressedDaysDir;

    @Override
    public void run() {
        // create AddressBookRegistry to load address books as needed during validation
        final AddressBookRegistry addressBookRegistry = new AddressBookRegistry();
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDaysDir == null) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }

        final Path statusFile = compressedDaysDir.toPath().resolve("validateCmdStatus.json");
        final Status resumeStatus = Status.readStatusFile(statusFile);

        final AtomicReference<byte[]> carryOverHash = new AtomicReference<>(ZERO_HASH);
        if (resumeStatus != null) {
            byte[] hb = resumeStatus.hashBytes();
            if (hb.length > 0) carryOverHash.set(hb);
            System.out.printf("Resuming at %s with hash[%s]%n",
                resumeStatus.recordFileTime,
                resumeStatus.endRunningHashHex.substring(0, 8));
        } else {
            System.out.println("Starting at genesis with hash[" + Bytes.wrap(carryOverHash.get()) + "]");
        }

        final AtomicReference<Status> lastGood = new AtomicReference<>(resumeStatus);

        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[]{compressedDaysDir});
        // Estimation/ETA support
        final long startNanos = System.nanoTime();
        final long INITIAL_ESTIMATE_PER_DAY = (24L * 60L * 60L) / 5L; // one every 5 seconds for a whole day
        final long dayCount = dayPaths.size();
        final AtomicLong totalProgress = new AtomicLong(dayCount * INITIAL_ESTIMATE_PER_DAY);
        final AtomicLong progress = new AtomicLong(0);

        // Producer-consumer queue with bounded capacity for backpressure
        final BlockingQueue<Item> queue = new LinkedBlockingQueue<>(100_000);

        // Register shutdown hook to persist last good status on JVM exit (Ctrl+C, etc.)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Status s = lastGood.get();
            if (s != null) {
                System.err.println("Shutdown: writing status to " + statusFile);
                Status.writeStatusFile(statusFile, s);
            }
        }, "validate-shutdown-hook"));

        // Reader thread: reads .tar.zstd day files and enqueues blocks
        final int startDay = Math.max(0, resumeStatus != null ? resumeStatus.dayIndex : 0);
        final Thread reader = new Thread(
                () -> {
                    try {
                        for (int day = startDay; day < dayPaths.size(); day++) {
                            final Path dayPath = dayPaths.get(day);
                            queue.put(Item.dayStart(day, dayPath));
                            try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                                final int dayIdx = day; // capture effectively final for lambda
                                // capture resumeStatus directly
                                stream.forEach(set -> {
                                     try {
                                         // If resuming and this is the resume day, skip blocks up to and including the
                                         // recorded recordFileTime
                                         if (resumeStatus != null && dayIdx == resumeStatus.dayIndex) {
                                            Instant ri = resumeStatus.recordInstant();
                                            if (!set.recordFileTime().isAfter(ri)) {
                                                // skip this block (it was already processed)
                                                return;
                                            }
                                         }
                                         queue.put(Item.block(dayIdx, dayPath, set));
                                     } catch (InterruptedException ie) {
                                         Thread.currentThread().interrupt();
                                         throw new RuntimeException(
                                                 "Interrupted while enqueueing block from " + dayPath, ie);
                                     }
                                });
                            } catch (Exception ex) {
                                PrettyPrint.clearProgress();
                                System.err.println("Failed processing day file: " + dayPath + ": " + ex.getMessage());
                                ex.printStackTrace();
                                // Persist status and exit
                                Status s = lastGood.get();
                                if (s != null) Status.writeStatusFile(statusFile, s);
                                System.exit(1);
                            }
                            // signal day end
                            queue.put(Item.dayEnd(day, dayPath));
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        PrettyPrint.clearProgress();
                        System.err.println("Reader thread interrupted");
                        Status s = lastGood.get();
                        if (s != null) Status.writeStatusFile(statusFile, s);
                        System.exit(1);
                    } finally {
                        try {
                            queue.put(Item.streamEnd());
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                "validate-reader");
        reader.start();

        long progressAtStartOfDay = 0L;
        int currentDay = -1;
        // Track the last consensus-minute (epoch minute) we reported progress for so we only print
        // once per minute of consensus time.
        long lastReportedMinute = Long.MIN_VALUE;

        try (FileWriter warningWriter = warningFile != null ? new FileWriter(warningFile, true) : null) {
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
                            // previous block hash from prior iteration (carry over)
                            final byte[] previousBlockHash = carryOverHash.get();
                            // Validate the block using InMemoryBlock.validate which performs internal checks
                            final ValidationResult vr =
                                    set.validate(previousBlockHash, addressBookRegistry.getCurrentAddressBook());
                            if (warningWriter != null && !vr.warningMessages().isEmpty()) {
                                warningWriter.write(
                                        "Warnings for " + set.recordFileTime() + ":\n" + vr.warningMessages() + "\n");
                                warningWriter.flush();
                            }
                            // check overall validity and fail if not valid
                            if (!vr.isValid()) {
                                PrettyPrint.clearProgress();
                                System.err.println(
                                        "Validation failed for " + set.recordFileTime() + ":\n" + vr.warningMessages());
                                System.out.flush();
                                // Persist last good and exit
                                Status s = lastGood.get();
                                if (s != null) Status.writeStatusFile(statusFile, s);
                                System.exit(1);
                            }
                            // use ValidationResult to update address book if needed
                            String addressBookChanges =
                                    addressBookRegistry.updateAddressBook(vr.addressBookTransactions());
                            if (warningWriter != null && addressBookChanges != null) {
                                warningWriter.write(addressBookChanges + "\n");
                                warningWriter.flush();
                            }
                            // update carry over to current block end-running-hash for next iteration
                            carryOverHash.set(vr.endRunningHash());

                            // Update last good processed block (in-memory only, not writing to disk here)
                            String endHashHex = HexFormat.of().formatHex(vr.endRunningHash());
                            lastGood.set(new Status(item.dayIndex, set.recordFileTime().toString(), endHashHex));

                            // Build progress string showing time and hashes (shortened to 8 chars for readability)
                            final String progressString = String.format(
                                    "%s carry[%s] next[%s]",
                                    set.recordFileTime(), shortHash(previousBlockHash), shortHash(vr.endRunningHash()));
                            // Estimate totals and ETA
                            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                            // Progress percent and remaining
                            final long processedSoFarAcrossAll = progress.get();
                            final long totalProgressFinal = totalProgress.get();
                            double percent = ((double) processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;
                            long remainingMillis =
                                    computeRemainingMilliseconds(processedSoFarAcrossAll, totalProgressFinal, elapsedMillis);
                            // Only print progress once per consensus-minute of blocks processed. Use epoch-second / 60
                            // to compute the minute bucket for the block's consensus time.
                            long blockMinute = set.recordFileTime().getEpochSecond() / 60L;
                            if (blockMinute != lastReportedMinute) {
                                PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                                lastReportedMinute = blockMinute;
                            }
                        } catch (Exception ex) {
                            PrettyPrint.clearProgress();
                            System.err.println("Validation threw for " + set.recordFileTime() + ": " + ex.getMessage());
                            ex.printStackTrace();
                            // Persist last good and exit
                            Status s = lastGood.get();
                            if (s != null) Status.writeStatusFile(statusFile, s);
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
            Status s = lastGood.get();
            if (s != null) Status.writeStatusFile(statusFile, s);
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // clear the progress line once done and print a summary
        PrettyPrint.clearProgress();
        System.out.println(
                "Validation complete. Days processed: " + dayCount + " , Blocks processed: " + progress.get());

        // On normal completion write final status (last good)
        Status sFinal = lastGood.get();
        if (sFinal != null) Status.writeStatusFile(statusFile, sFinal);

        // Ensure reader finished
        try {
            reader.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Compute remaining milliseconds based on progress so far, total progress expected, and elapsed time.
     *
     * @param processedSoFarAcrossAll number of units processed so far
     * @param totalProgressFinal      total number of units expected
     * @param elapsedMillis           elapsed time in milliseconds
     * @return estimated remaining time in milliseconds
     */
    private static long computeRemainingMilliseconds(
            long processedSoFarAcrossAll, long totalProgressFinal, long elapsedMillis) {
        long remainingMillis;
        if (processedSoFarAcrossAll > 0) {
            long remainingUnits = totalProgressFinal - processedSoFarAcrossAll;
            remainingMillis = (long) ((elapsedMillis * (double) remainingUnits) / (double) processedSoFarAcrossAll);
            if (remainingMillis < 0) {
                remainingMillis = 0;
            }
        } else {
            remainingMillis = Long.MAX_VALUE; // unknown ETA at the very start
        }
        return remainingMillis;
    }

    /**
     * Shorten a hash to prefix 8 chars of hex for readability
     *
     * @param hash the hash to shorten
     * @return the shortened hash
     */
    private static String shortHash(final byte[] hash) {
        if (hash == null) return "null";
        final String s = Bytes.wrap(hash).toString();
        return s.length() <= 8 ? s : s.substring(0, 8);
    }
}
