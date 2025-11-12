// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.time.ZoneOffset.UTC;
import static org.hiero.block.tools.commands.days.model.TarZstdDayUtils.parseDayFromFileName;

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
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;
import org.hiero.block.tools.commands.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.commands.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.RecordFileBlock;
import org.hiero.block.tools.records.RecordFileBlock.ValidationResult;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

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
     * @param dayDate  the date of the day being processed
     * @param dayFile  for diagnostics (maybe null for STREAM_END)
     * @param block    only for BLOCK
     */
     private record Item(Kind kind, LocalDate dayDate, Path dayFile, RecordFileBlock block) {
        enum Kind {
            DAY_START,
            BLOCK,
            DAY_END,
            STREAM_END
        }

        static Item dayStart(LocalDate date, Path file) {
            return new Item(Kind.DAY_START, date, file, null);
        }

        static Item block(LocalDate date, Path file, RecordFileBlock b) {
            return new Item(Kind.BLOCK, date, file, b);
        }

        static Item dayEnd(LocalDate date, Path file) {
            return new Item(Kind.DAY_END, date, file, null);
        }

        static Item streamEnd() {
            return new Item(Kind.STREAM_END, null, null, null);
        }
    }

    /**
     * Simple status object saved to compressedDaysDir/validateCmdStatus.json to allow resuming.
     * <p>Note: JSON (de)serialization handled by Gson via field reflection.</p>
     */
    @SuppressWarnings("ClassCanBeRecord")
    private static final class Status {
        final String dayDate; // ISO-8601 LocalDate (e.g., "2019-09-13")
        final String recordFileTime; // ISO-8601 from Instant.toString()
        final String endRunningHashHex;

        Status(LocalDate dayDate, String recordFileTime, String endRunningHashHex) {
            this.dayDate = dayDate.toString();
            this.recordFileTime = recordFileTime;
            this.endRunningHashHex = endRunningHashHex;
        }

        LocalDate dayLocalDate() {
            return LocalDate.parse(dayDate);
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

    @Option(
            names = {"-d", "--start-date"},
            description = "Start validation from this date (format: YYYY-MM-DD), ignoring any resume status file")
    private String startDate = null;

    @Option(
            names = {"--use-jni"},
            description = "Use zstd-jni library instead of subprocess for decompression")
    private boolean useJni = false;

    @Parameters(index = "0", description = "Directories of days to process")
    @SuppressWarnings("unused") // assigned reflectively by picocli
    private File compressedDaysDir;

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDaysDir == null) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        // create AddressBookRegistry to load address books as needed during validation
        final Path addressBookFile = compressedDaysDir.toPath().resolve("addressBookHistory.json");
        final AddressBookRegistry addressBookRegistry = Files.exists(addressBookFile) ?
            new AddressBookRegistry(addressBookFile) : new AddressBookRegistry();
        // load mirror node day info if available (not required)
        Map<LocalDate, DayBlockInfo> tempDayInfo;
        try {
            tempDayInfo = DayBlockInfo.loadDayBlockInfoMap();
        } catch (Exception e) {
            System.out.println("Failed to load day block info map so ignoring");
            tempDayInfo = null;
        }
        final Map<LocalDate, DayBlockInfo> dayInfo = tempDayInfo;
        // load resume status if available
        final Path statusFile = compressedDaysDir.toPath().resolve("validateCmdStatus.json");
        final Status resumeStatus = Status.readStatusFile(statusFile);
        // atomic reference for last good status
        final AtomicReference<Status> lastGood = new AtomicReference<>(resumeStatus);
        // load all the day paths
        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[]{compressedDaysDir});
        if (dayPaths.isEmpty()) {
            System.out.println("No day files found in " + compressedDaysDir);
            return;
        }
        // atomic reference for carry over hash between blocks
        final AtomicReference<byte[]> carryOverHash = new AtomicReference<>();

        // Determine the starting date (from --start-date option or resume status)
        LocalDate actualStartDate = null;
        if (startDate != null) {
            try {
                actualStartDate = LocalDate.parse(startDate);
                System.out.println("Starting from date: " + actualStartDate);
            } catch (Exception e) {
                System.err.println("Invalid start date format: " + startDate + ". Expected YYYY-MM-DD");
                System.exit(1);
            }
        } else if (resumeStatus != null) {
            actualStartDate = resumeStatus.dayLocalDate();
        }

        // Set up carry over hash based on starting point
        if (resumeStatus != null && startDate == null) {
            // Normal resume from status file
            byte[] hb = resumeStatus.hashBytes();
            if (hb.length > 0) carryOverHash.set(hb);
            System.out.printf("Resuming at %s with hash[%s]%n",
                resumeStatus.recordFileTime,
                resumeStatus.endRunningHashHex.substring(0, 8));
        } else if (actualStartDate != null) {
            // Starting from explicit date - try to get prior day hash from mirror node
            if (actualStartDate.equals(LocalDate.parse("2019-09-13"))) {
                carryOverHash.set(ZERO_HASH);
                System.out.println("Starting at genesis with hash[" + Bytes.wrap(carryOverHash.get()) + "]");
            } else if (dayInfo != null) {
                LocalDate priorDayDate = actualStartDate.minusDays(1);
                DayBlockInfo priorDayInfo = dayInfo.get(priorDayDate);
                if (priorDayInfo != null) {
                    byte[] priorHash = HexFormat.of().parseHex(priorDayInfo.lastBlockHash);
                    carryOverHash.set(priorHash);
                    System.out.printf("Starting at %s with mirror node last hash[%s]%n",
                        actualStartDate,
                        Bytes.wrap(carryOverHash.get()));
                } else {
                    carryOverHash.set(null);
                    System.out.println("No prior day info for " + priorDayDate +
                        ", cannot validate first block's previous hash");
                }
            }
        } else {
            // Starting from beginning without resume
            if (dayPaths.getFirst().getFileName().toString().startsWith("2019-09-13")) {
                carryOverHash.set(ZERO_HASH);
                System.out.println("Starting at genesis with hash[" + Bytes.wrap(carryOverHash.get()) + "]");
            } else if (dayInfo != null) {
                // not starting at genesis, so try to get prior day's last block hash from mirror node data
                LocalDate firstDayDate = parseDayFromFileName(dayPaths.getFirst().getFileName().toString());
                LocalDate priorDayDate = firstDayDate.minusDays(1);
                DayBlockInfo priorDayInfo = dayInfo.get(priorDayDate);
                if (priorDayInfo != null) {
                    byte[] priorHash = HexFormat.of().parseHex(priorDayInfo.lastBlockHash);
                    carryOverHash.set(priorHash);
                    System.out.printf("Starting at %s with mirror node last hash[%s]%n",
                        firstDayDate,
                        Bytes.wrap(carryOverHash.get()));
                } else {
                    carryOverHash.set(null);
                    System.out.println("No prior day info for " + priorDayDate +
                        ", cannot validate first block's previous hash");
                }
            }
        }
        // Estimation/ETA support
        final long startNanos = System.nanoTime();
        final long INITIAL_ESTIMATE_PER_DAY = (24L * 60L * 60L) / 5L; // one every 5 seconds for a whole day
        final long dayCount = dayPaths.size();
        final AtomicLong totalProgress = new AtomicLong(dayCount * INITIAL_ESTIMATE_PER_DAY);
        final AtomicLong progress = new AtomicLong(0);

        // Producer-consumer queue with bounded capacity for backpressure
        final BlockingQueue<Item> queue = new LinkedBlockingQueue<>(1_000);

        // Register shutdown hook to persist last good status on JVM exit (Ctrl+C, etc.)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Status s = lastGood.get();
            if (s != null) {
                System.err.println("Shutdown: writing status to " + statusFile);
                Status.writeStatusFile(statusFile, s);
                System.err.println("Shutdown: address book to " + addressBookFile);
                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
            }
        }, "validate-shutdown-hook"));

        // Reader thread: reads .tar.zstd day files and enqueues blocks
        // Determine which day index to start from (based on --start-date or resume status)
        final int startDay;
        if (actualStartDate != null) {
            // Find the day file matching the start date (either from --start-date or resume status)
            int foundIndex = -1;
            for (int i = 0; i < dayPaths.size(); i++) {
                LocalDate dayDate = parseDayFromFileName(dayPaths.get(i).getFileName().toString());
                if (dayDate.equals(actualStartDate) || dayDate.isAfter(actualStartDate)) {
                    foundIndex = i;
                    break;
                }
            }
            if (foundIndex < 0) {
                System.err.println("Warning: Could not find day file for " + actualStartDate + " or later, starting from beginning");
                startDay = 0;
            } else {
                startDay = foundIndex;
                LocalDate actualDay = parseDayFromFileName(dayPaths.get(foundIndex).getFileName().toString());
                if (!actualDay.equals(actualStartDate)) {
                    System.out.println("Note: Exact date " + actualStartDate + " not found, starting from " + actualDay);
                }
            }
        } else {
            startDay = 0;
        }

        final Thread reader = new Thread(
                () -> {
                    try {
                        for (int day = startDay; day < dayPaths.size(); day++) {
                            final Path dayPath = dayPaths.get(day);
                            final LocalDate dayDate = parseDayFromFileName(dayPath.getFileName().toString());
                            queue.put(Item.dayStart(dayDate, dayPath));
                            try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath, useJni)) {
                                stream.forEach(set -> {
                                     try {
                                         // If resuming from status file (not --start-date) and this is the resume day,
                                         // skip blocks up to and including the recorded recordFileTime
                                         if (resumeStatus != null && startDate == null && dayDate.equals(resumeStatus.dayLocalDate())) {
                                            Instant ri = resumeStatus.recordInstant();
                                            if (!set.recordFileTime().isAfter(ri)) {
                                                // skip this block (it was already processed)
                                                return;
                                            }
                                         }
                                         queue.put(Item.block(dayDate, dayPath, set));
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
                                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                System.exit(1);
                            }
                            // signal day end
                            queue.put(Item.dayEnd(dayDate, dayPath));
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        PrettyPrint.clearProgress();
                        System.err.println("Reader thread interrupted");
                        Status s = lastGood.get();
                        if (s != null) Status.writeStatusFile(statusFile, s);
                        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
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
        int currentDayCount = 0; // Track number of days processed so far
        // Track the last consensus-minute (epoch minute) we reported progress for so we only print
        // once per minute of consensus time.
        long lastReportedMinute = Long.MIN_VALUE;

        try (FileWriter warningWriter = warningFile != null ? new FileWriter(warningFile, true) : null) {
            final AtomicLong blockInDayCounter = new AtomicLong(0L);
            // Consumer loop: validate blocks and update progress/ETA
            while (true) {
                final Item item = queue.take();
                if (item.kind == Item.Kind.STREAM_END) break;

                switch (item.kind) {
                    case DAY_START -> {
                        progressAtStartOfDay = progress.get();
                        blockInDayCounter.set(0L);
                    }
                    case BLOCK -> {
                        final RecordFileBlock block = item.block;
                        // update counters
                        progress.incrementAndGet();
                        try {
                            // previous block hash from prior iteration (carry over)
                            final byte[] previousBlockHash = carryOverHash.get();
                            // Validate the block using RecordFileBlock.validate which performs internal checks
                            final ValidationResult vr =
                                    block.validate(previousBlockHash, addressBookRegistry.getCurrentAddressBook());
                            if (warningWriter != null && !vr.warningMessages().isEmpty()) {
                                warningWriter.write(
                                        "Warnings for " + block.recordFileTime() + ":\n" + vr.warningMessages() + "\n");
                                warningWriter.flush();
                            }
                            // check overall validity and fail if not valid
                            if (!vr.isValid()) {
                                PrettyPrint.clearProgress();
                                System.err.println(
                                        "Validation failed for " + block.recordFileTime() + ":\n" + vr.warningMessages());
                                System.out.flush();
                                // Persist last good and exit
                                Status s = lastGood.get();
                                if (s != null) Status.writeStatusFile(statusFile, s);
                                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                System.exit(1);
                            }
                            // check if this is the first block of the first day and validate hash against mirror
                            // node data if so
                            if (blockInDayCounter.get() == 0L && dayInfo != null) {
                                // we are the first block of a day (not the first day), so we have computed the prior
                                // day's last hash, so we can compare that to one from mirror node data if available
                                LocalDate dayDate = parseDayFromFileName(item.dayFile.getFileName().toString());
                                DayBlockInfo thisDaysInfo = dayInfo.get(dayDate);
                                // make sure the block is the first block of the day by checking its time is within
                                // 10 seconds of midnight
                                if (block.recordFileTime().isBefore(
                                        dayDate.atStartOfDay(UTC).plusSeconds(10).toInstant())) {
                                    // now we can compare hashes
                                    byte[] expectedHash = HexFormat.of().parseHex(thisDaysInfo.firstBlockHash);
                                    if (!Arrays.equals(vr.endRunningHash(), expectedHash)) {
                                        PrettyPrint.clearProgress();
                                        System.err.printf(
                                            "Validation failed for %s: first block of day has previous hash[%s] but "
                                                + "expected[%s] from mirror node data%n",
                                            dayDate,
                                            Bytes.wrap(vr.endRunningHash()),
                                            Bytes.wrap(expectedHash));
                                        System.out.flush();
                                        // Persist last good and exit
                                        Status s = lastGood.get();
                                        if (s != null)
                                            Status.writeStatusFile(statusFile, s);
                                        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                        System.exit(1);
                                    }
                                }
                            }
                            // use ValidationResult to update address book if needed
                            String addressBookChanges =
                                    addressBookRegistry.updateAddressBook(block.recordFileTime(), vr.addressBookTransactions());
                            if (warningWriter != null && addressBookChanges != null) {
                                warningWriter.write(addressBookChanges + "\n");
                                warningWriter.flush();
                            }
                            // update carry over to current block end-running-hash for next iteration
                            carryOverHash.set(vr.endRunningHash());

                            // Update last good processed block (in-memory only, not writing to disk here)
                            String endHashHex = HexFormat.of().formatHex(vr.endRunningHash());
                            lastGood.set(new Status(item.dayDate, block.recordFileTime().toString(), endHashHex));

                            // Build progress string showing time and hashes (shortened to 8 chars for readability)
                            final String progressString = String.format(
                                    "%s carry[%s] next[%s]",
                                    block.recordFileTime(), shortHash(previousBlockHash), shortHash(vr.endRunningHash()));
                            // Estimate totals and ETA
                            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                            // Progress percent and remaining
                            final long processedSoFarAcrossAll = progress.get();
                            final long totalProgressFinal = totalProgress.get();
                            double percent = ((double) processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;
                            long remainingMillis =
                                    PrettyPrint.computeRemainingMilliseconds(processedSoFarAcrossAll, totalProgressFinal, elapsedMillis);
                            // Only print progress once per consensus-minute of blocks processed. Use epoch-second / 60
                            // to compute the minute bucket for the block's consensus time.
                            long blockMinute = block.recordFileTime().getEpochSecond() / 60L;
                            if (blockMinute != lastReportedMinute) {
                                PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                                lastReportedMinute = blockMinute;
                            }
                        } catch (Exception ex) {
                            PrettyPrint.clearProgress();
                            System.err.println("Validation threw for " + block.recordFileTime() + ": " + ex.getMessage());
                            ex.printStackTrace();
                            // Persist last good and exit
                            Status s = lastGood.get();
                            if (s != null) Status.writeStatusFile(statusFile, s);
                            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                            System.exit(1);
                        }
                        blockInDayCounter.incrementAndGet();
                    }
                    case DAY_END -> {
                        // After finishing the day, update aggregates
                        currentDayCount++;
                        final long progressAtEndOfDay = progress.get();
                        final long blocksInDay = progressAtEndOfDay - progressAtStartOfDay;
                        final long remainingDays = dayCount - currentDayCount;
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
            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
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
        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);

        // Ensure reader finished
        try {
            reader.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
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
