// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hiero.block.tools.commands.mirrornode.BlockInfo;
import org.hiero.block.tools.commands.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.commands.mirrornode.MirrorNodeBlockQueryOrder;
import org.hiero.block.tools.records.RecordFileInfo;
import org.hiero.block.tools.utils.Gzip;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI implementation for the {@code days download-live} command.
 *
 * <p>This command parses and validates arguments, then runs a day-scoped poll loop that:
 * <ul>
 *   <li>Queries the mirror node for recent blocks using the {@code /api/v1/blocks} endpoint</li>
 *   <li>Filters results to the current day window in the configured rollover timezone</li>
 *   <li>Downloads, validates and organises record files into per-day folders under {@code --out}</li>
 *   <li>Appends successfully validated files into a per-day {@code &lt;dayKey&gt;.tar} archive</li>
 *   <li>Compresses completed day archives to {@code .tar.zstd} and cleans up loose files</li>
 *   <li>Persists {@code dayKey} and {@code lastSeenBlock} to a small JSON file for resumable operation</li>
 * </ul>
 *
 * <p>The behaviour of the poller is controlled by the optional {@code --start-day} and
 * {@code --end-day} flags, which define a global ingestion window. The following modes are
 * supported:
 *
 * <h3>1. Start + end date (finite historical range)</h3>
 * <ul>
 *   <li>Specify both {@code --start-day} and {@code --end-day}</li>
 *   <li>The mirror node query applies {@code timestamp &gt;= startDayT00:00} and
 *       {@code timestamp &lt; (endDay + 1)T00:00} as Unix {@code seconds.nanoseconds} filters</li>
 *   <li>Locally, the tool still rolls over at each midnight, building one tar per day and then
 *       compressing it to {@code .tar.zstd} and deleting the per-day folder</li>
 *   <li>This is useful for backfilling a bounded historical window</li>
 * </ul>
 *
 * <h3>2. Start date only (catch-up then follow live)</h3>
 * <ul>
 *   <li>Specify {@code --start-day}, but omit {@code --end-day}</li>
 *   <li>The mirror node query applies only a lower bound:
 *       {@code timestamp &gt;= startDayT00:00}</li>
 *   <li>The poller walks forward day-by-day from the start date until it reaches the present,
 *       then naturally continues following new blocks as they arrive</li>
 *   <li>Suitable for "bootstrap from this date and then stay live"</li>
 * </ul>
 *
 * <h3>3. No start/end date (pure live mode)</h3>
 * <ul>
 *   <li>If neither {@code --start-day} nor {@code --end-day} is supplied, the poller starts
 *       from "today" in {@code --day-rollover-tz}</li>
 *   <li>A lower bound is applied at today's midnight; the tool then tracks new blocks as they
 *       appear, rolling over archives at each midnight</li>
 * </ul>
 *
 * <p>In all modes, {@code lastSeenBlock} is treated as a global, monotonically increasing
 * sequence number shared across days. This ensures the downloader never re-processes blocks
 * whose numbers are less than or equal to the last successfully processed block, even when
 * crossing day boundaries or restarting from persisted state.</p>
 *
 * <p>Business logic such as detailed hash verification and reuse of the historic {@code download2}
 * fetcher is wired via the {@link LiveDownloader} inner class.</p>
 */
@Command(
        name = "download-live",
        description =
                "Continuously follow mirror node for new block files; dedupe, validate, and organize into daily folders.",
        mixinStandardHelpOptions = true,
        version = "download-live 0.1")
public class DownloadLive implements Runnable {

    @Option(
            names = "--out",
            required = true,
            paramLabel = "DIR",
            description = "Output directory for daily folders (e.g., /data/records)")
    private Path out;

    @Option(
            names = "--source-root",
            required = true,
            paramLabel = "source folder root",
            description = "File storage location for record streams (e.g., /mnt/days/folder)")
    private String sourceRoot;

    @Option(
            names = "--poll-interval",
            defaultValue = "60s",
            paramLabel = "DURATION",
            description = "Polling interval for mirror API (e.g., 60s, 2m). Parsed later by implementation.")
    private String pollInterval;

    @Option(
            names = "--batch-size",
            defaultValue = "100",
            paramLabel = "N",
            description = "Max number of block descriptors to request per poll (mirror max is typically 100).")
    private int batchSize;

    @Option(
            names = "--day-rollover-tz",
            defaultValue = "UTC",
            paramLabel = "TZ",
            description = "Timezone ID used to determine end-of-day rollover (e.g., UTC, America/Los_Angeles).")
    private String dayRolloverTz;

    @Option(
            names = "--start-day",
            paramLabel = "YYYY-MM-DD",
            description =
                    "Optional start day (inclusive) for ingestion, e.g., 2025-11-10. Defaults to the current day in the rollover timezone.")
    private String startDay;

    @Option(
            names = "--end-day",
            paramLabel = "YYYY-MM-DD",
            description =
                    "Optional end day (inclusive) for ingestion, e.g., 2025-11-15. If omitted, ingestion continues indefinitely and rolls over each day.")
    private String endDay;

    @Option(names = "--max-concurrency", defaultValue = "8", paramLabel = "N", description = "Max parallel downloads.")
    private int maxConcurrency;

    @Option(names = "--run-poller", defaultValue = "false", description = "If true, run the day-scoped live poller.")
    private boolean runPoller;

    @Option(
            names = "--state-json",
            defaultValue = "./state/download-live.json",
            paramLabel = "FILE",
            description = "Path to a small JSON file used to persist last-seen state for resume.")
    private Path stateJsonPath;

    @Option(
            names = "--tmp-dir",
            defaultValue = "./tmp/download-live",
            paramLabel = "DIR",
            description = "Temporary directory used for streaming downloads before atomic move into the day folder.")
    private Path tmpDir;

    @Override
    public void run() {
        System.out.println("[download-live] Starting");
        System.out.println("Configuration:");
        System.out.println("  out=" + out);
        System.out.println("  sourceRoot=" + sourceRoot);
        System.out.println("  pollInterval=" + pollInterval);
        System.out.println("  batchSize=" + batchSize);
        System.out.println("  dayRolloverTz=" + dayRolloverTz);
        System.out.println("  startDay=" + startDay);
        System.out.println("  endDay=" + endDay);
        System.out.println("  maxConcurrency=" + maxConcurrency);
        System.out.println("  runPoller=" + runPoller);
        System.out.println("  stateJsonPath=" + stateJsonPath);
        System.out.println("  tmpDir=" + tmpDir);

        if (!runPoller) {
            System.out.println("Status: Ready (use --run-poller to start the day-scoped poll loop)");
            return;
        }

        // --- Start day-scoped poller with live downloader ---
        final ZoneId tz = ZoneId.of(dayRolloverTz);
        final Duration interval = parseHumanDuration(pollInterval);
        final LiveDownloader downloader = new LiveDownloader(out, tmpDir, sourceRoot, maxConcurrency);

        LocalDate startDayParsed = null;
        LocalDate endDayParsed = null;
        try {
            if (startDay != null && !startDay.isBlank()) {
                startDayParsed = LocalDate.parse(startDay.trim());
            }
            if (endDay != null && !endDay.isBlank()) {
                endDayParsed = LocalDate.parse(endDay.trim());
            }
        } catch (Exception e) {
            throw new CommandLine.ParameterException(
                    new CommandLine(new DownloadLive()),
                    "Invalid --start-day/--end-day; expected format YYYY-MM-DD. startDay="
                            + startDay + " endDay=" + endDay);
        }

        final LivePoller poller =
                new LivePoller(interval, tz, batchSize, stateJsonPath, downloader, startDayParsed, endDayParsed);
        System.out.println("[download-live] Starting LivePoller (continuous; press Ctrl-C to stop)...");
        poller.runContinuouslyForToday();
    }

    /**
     * Supported compression formats for daily archives.
     * Chosen names match common archive naming in the project.
     */
    public enum CompressFormat {
        tar_zstd("tar.zstd"),
        tar_gz("tar.gz");

        private final String label;

        CompressFormat(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    private static Duration parseHumanDuration(String text) {
        // Accepts forms like 60s, 2m, 1h, or ISO-8601 (PT1M)
        try {
            if (text.endsWith("ms")) {
                long ms = Long.parseLong(text.substring(0, text.length() - 2));
                return Duration.ofMillis(ms);
            } else if (text.endsWith("s")) {
                long s = Long.parseLong(text.substring(0, text.length() - 1));
                return Duration.ofSeconds(s);
            } else if (text.endsWith("m")) {
                long m = Long.parseLong(text.substring(0, text.length() - 1));
                return Duration.ofMinutes(m);
            } else if (text.endsWith("h")) {
                long h = Long.parseLong(text.substring(0, text.length() - 1));
                return Duration.ofHours(h);
            } else {
                return Duration.parse(text); // e.g., PT1M
            }
        } catch (Exception e) {
            throw new CommandLine.ParameterException(
                    new CommandLine(new DownloadLive()),
                    "Invalid duration: " + text + " (use forms like 60s, 2m, 1h, PT1M)");
        }
    }

    /**
     * Parse a mirror timestamp string like "1762898218.515837000" into an Instant.
     * Returns null if input is null or malformed.
     */
    private static Instant parseMirrorTimestamp(String ts) {
        if (ts == null || ts.isEmpty()) return null;
        try {
            int dot = ts.indexOf('.');
            if (dot < 0) {
                long seconds = Long.parseLong(ts);
                return Instant.ofEpochSecond(seconds, 0);
            }
            long seconds = Long.parseLong(ts.substring(0, dot));
            String nanoStr = ts.substring(dot + 1);
            // Normalize nanos to 9 digits
            if (nanoStr.length() > 9) nanoStr = nanoStr.substring(0, 9);
            while (nanoStr.length() < 9) nanoStr += "0";
            int nanos = Integer.parseInt(nanoStr);
            return Instant.ofEpochSecond(seconds, nanos);
        } catch (Exception e) {
            return null;
        }
    }

    // --- Simple JSON state persistence (no external libs) ---

    private static final Pattern P_DAY = Pattern.compile("\"dayKey\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern P_LAST = Pattern.compile("\"lastSeenBlock\"\\s*:\\s*(\\d+)");

    private static State readState(Path path) {
        try {
            if (path == null) return null;
            if (!Files.exists(path)) return null;
            String s = Files.readString(path, StandardCharsets.UTF_8);
            Matcher mDay = P_DAY.matcher(s);
            Matcher mLast = P_LAST.matcher(s);
            String day = mDay.find() ? mDay.group(1) : null;
            long last = mLast.find() ? Long.parseLong(mLast.group(1)) : -1L;
            if (day == null) return null;
            return new State(day, last);
        } catch (Exception e) {
            System.err.println("[poller] Failed to read state: " + e.getMessage());
            return null;
        }
    }

    private static void writeState(Path path, State st) {
        if (path == null || st == null) return;
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            String json = "{\n" + "  \"dayKey\": \""
                    + st.dayKey + "\",\n" + "  \"lastSeenBlock\": "
                    + st.lastSeenBlock + "\n" + "}\n";
            Files.writeString(path, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[poller] Failed to write state: " + e.getMessage());
        }
    }

    private static final class State {
        final String dayKey;
        final long lastSeenBlock;

        State(String dayKey, long lastSeenBlock) {
            this.dayKey = dayKey;
            this.lastSeenBlock = lastSeenBlock;
        }
    }

    // --- Live poller ---

    /**
     * Day-scoped live poller that queries the mirror node for latest blocks,
     * filters to the current day + unseen blocks, then delegates to the
     * LiveDownloader to fetch and place files.
     */
    static final class LivePoller {
        private final Duration interval;
        private final ZoneId tz;
        private final int batchSize;
        private long lastSeenBlock = -1L;
        private final Path statePath;
        private final LiveDownloader downloader;
        private boolean stateLoadedForToday = false;
        // Optional global date range for ingestion.
        private final LocalDate configuredStartDay;
        private final LocalDate configuredEndDay;

        LivePoller(
                Duration interval,
                ZoneId tz,
                int batchSize,
                Path statePath,
                LiveDownloader downloader,
                LocalDate configuredStartDay,
                LocalDate configuredEndDay) {
            this.interval = interval;
            this.tz = tz;
            this.batchSize = batchSize;
            this.statePath = statePath;
            this.downloader = downloader;
            this.configuredStartDay = configuredStartDay;
            this.configuredEndDay = configuredEndDay;
        }

        void runOnceForCurrentDay() {
            final ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), tz);
            final LocalDate day = now.toLocalDate();
            final ZonedDateTime start = day.atStartOfDay(tz);
            final ZonedDateTime end = start.plusDays(1);
            final String dayKey = day.toString(); // YYYY-MM-DD

            if (!stateLoadedForToday) {
                State st = readState(statePath);
                if (st != null && dayKey.equals(st.dayKey)) {
                    lastSeenBlock = st.lastSeenBlock;
                    System.out.println("[poller] Resumed lastSeenBlock from state: " + lastSeenBlock);
                } else {
                    System.out.println("[poller] No matching state for " + dayKey + " (starting fresh).");
                }
                stateLoadedForToday = true;
            }
            System.out.println("[poller] dayKey=" + dayKey + " interval=" + interval + " batchSize=" + batchSize
                    + " lastSeen=" + lastSeenBlock);

            // Build Mirror Node timestamp filters for the global ingestion window.
            // We always apply a lower bound at the configured start day (or today's day if not configured).
            // If an end day is configured, we also bound the query to be strictly before endDay+1 midnight.
            final LocalDate lowerBoundDay = (configuredStartDay != null) ? configuredStartDay : day;
            final long startSeconds = lowerBoundDay.atStartOfDay(tz).toEpochSecond();

            final List<String> timestampFilters = new ArrayList<>();
            timestampFilters.add("gte:" + startSeconds + ".000000000");

            if (configuredEndDay != null) {
                final long endSeconds =
                        configuredEndDay.plusDays(1).atStartOfDay(tz).toEpochSecond();
                timestampFilters.add("lt:" + endSeconds + ".000000000");
            }

            // Fetch latest blocks (descending) and filter to the current day + unseen.
            final List<BlockInfo> latest = FetchBlockQuery.getLatestBlocks(batchSize, MirrorNodeBlockQueryOrder.DESC, timestampFilters);
            final List<BlockDescriptor> batch = new ArrayList<>();
            for (BlockInfo b : latest) {
                long number = b.number;
                if (lastSeenBlock >= 0 && number <= lastSeenBlock) {
                    continue;
                }
                // Prefer timestamp.from; fallback to timestamp.to
                Instant ts = parseMirrorTimestamp(b.timestampFrom != null ? b.timestampFrom : b.timestampTo);
                if (ts == null) {
                    continue; // skip if no timestamp to evaluate
                }
                ZonedDateTime zts = ZonedDateTime.ofInstant(ts, tz);
                if (zts.isBefore(start) || !zts.isBefore(end)) {
                    continue; // outside current day window
                }
                String hash = b.hash;
                String name = b.name;
                String iso = zts.toString();
                batch.add(new BlockDescriptor(number, name, iso, hash));
            }

            // Sort asc so we process from oldest to newest, then hand off to the downloader
            batch.sort((a, b) -> Long.compare(a.blockNumber, b.blockNumber));
            System.out.println("[poller] descriptors=" + batch.size());
            if (!batch.isEmpty()) {
                final long highestDownloaded = downloader.downloadBatch(dayKey, batch);
                if (highestDownloaded > lastSeenBlock) {
                    lastSeenBlock = highestDownloaded;
                }
                batch.stream()
                        .limit(3)
                        .forEach(d -> System.out.println("[poller] sample -> block=" + d.blockNumber + " file="
                                + d.filename + " ts=" + d.timestampIso));
                // Persist state for resume
                writeState(statePath, new State(dayKey, lastSeenBlock));
            } else {
                System.out.println("[poller] No new blocks this tick.");
            }
        }

        void runContinuouslyForToday() {
            String currentDayKey =
                    ZonedDateTime.ofInstant(Instant.now(), tz).toLocalDate().toString();
            while (true) {
                final String dayKey =
                        ZonedDateTime.ofInstant(Instant.now(), tz).toLocalDate().toString();
                if (!dayKey.equals(currentDayKey)) {
                    System.out.println("[poller] Day changed (" + currentDayKey + " -> " + dayKey
                            + "); finalizing previous day and rolling over.");
                    try {
                        downloader.finalizeDay(currentDayKey);
                    } catch (Exception e) {
                        System.err.println(
                                "[poller] Failed to finalize day archive for " + currentDayKey + ": " + e.getMessage());
                    }
                    // rollover: start tracking the new day; state will be reloaded for the new key
                    currentDayKey = dayKey;
                    stateLoadedForToday = false;
                }
                try {
                    runOnceForCurrentDay();
                } catch (Exception e) {
                    System.err.println("[poller] Tick failed: " + e.getMessage());
                }
                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Handles downloading and placing files for a batch of blocks.
     *
     * For now this uses a small, self-contained implementation that:
     *  - Creates a per-day output directory under outRoot/dayKey
     *  - Streams content into a temp file under tmpRoot
     *  - Atomically moves the temp file into the final target path
     *
     * The body of {@link #downloadSingle(String, BlockDescriptor)} is the place to hook in the
     * existing "download2" fetcher and hash validation logic so that this live flow reuses the
     * same streaming + validation guarantees as the day-based tooling.
     */
    static final class LiveDownloader {
        private final Path outRoot;
        private final Path tmpRoot;
        private final String sourceRoot;
        private final int maxConcurrency;
        // Single-threaded executor used for background compression of per-day tar files.
        private final ExecutorService compressionExecutor;

        LiveDownloader(Path outRoot, Path tmpRoot, String sourceRoot, int maxConcurrency) {
            this.outRoot = outRoot;
            this.tmpRoot = tmpRoot;
            this.sourceRoot = sourceRoot;
            this.maxConcurrency = Math.max(1, maxConcurrency);
            this.compressionExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "download-live-compress");
                t.setDaemon(true);
                return t;
            });
        }

        /**
         * Append the given file (by entryName) to the per-day tar archive using the system tar command.
         * The tar file is created under outRoot as <dayKey>.tar and entries are taken from the per-day folder.
         */
        void appendToDayTar(String dayKey, String entryName) {
            try {
                final Path dayDir = outRoot.resolve(dayKey);
                final Path tarPath = outRoot.resolve(dayKey + ".tar");

                if (!Files.isDirectory(dayDir)) {
                    // Nothing to do if the day directory doesn't exist yet.
                    return;
                }

                final boolean tarExists = Files.exists(tarPath);
                final ProcessBuilder pb;
                if (!tarExists) {
                    // First entry: create tar with the initial file.
                    pb = new ProcessBuilder("tar", "-cf", tarPath.toString(), entryName);
                } else {
                    // Append to existing tar.
                    pb = new ProcessBuilder("tar", "-rf", tarPath.toString(), entryName);
                }
                pb.directory(dayDir.toFile());
                final Process p = pb.start();
                final int exit = p.waitFor();
                if (exit != 0) {
                    System.err.println("[download] tar command failed for day " + dayKey + " entry " + entryName
                            + " with exit=" + exit);
                } else {
                    System.out.println("[download] appended " + entryName + " to " + tarPath);
                }
            } catch (Exception e) {
                System.err.println("[download] Failed to append to tar for day " + dayKey + ": " + e.getMessage());
            }
        }

        /**
         * Schedule finalization of a day's archive on a background thread.
         * This "closes" the tar for the day by stopping further appends (handled by the poller/dayKey rollover),
         * then compresses <dayKey>.tar into <dayKey>.tar.zstd and cleans up the per-day folder.
         */
        void finalizeDay(String dayKey) {
            System.out.println("[download] Scheduling background compression for day " + dayKey);
            compressionExecutor.submit(() -> compressAndCleanupDay(dayKey));
        }

        /**
         * Worker that runs in the background executor to compress and clean up a day's data.
         */
        private void compressAndCleanupDay(String dayKey) {
            try {
                final Path tarPath = outRoot.resolve(dayKey + ".tar");
                final Path dayDir = outRoot.resolve(dayKey);
                if (!Files.isRegularFile(tarPath)) {
                    System.out.println("[download] No tar file for day " + dayKey + " to compress; skipping.");
                    return;
                }
                final Path zstdPath = outRoot.resolve(dayKey + ".tar.zstd");
                System.out.println("[download] Compressing " + tarPath + " -> " + zstdPath + " using zstd");
                final ProcessBuilder pb = new ProcessBuilder(
                        "zstd",
                        "-T0", // use all cores
                        "-f", // overwrite output if it exists
                        tarPath.toString(),
                        "-o",
                        zstdPath.toString());
                pb.inheritIO();
                final Process p = pb.start();
                final int exit = p.waitFor();
                if (exit != 0) {
                    System.err.println("[download] zstd compression failed for " + tarPath + " with exit=" + exit);
                } else {
                    System.out.println("[download] zstd compression complete for " + tarPath);
                    // Clean up individual per-day files now that we have tar and tar.zstd.
                    // It makes sense to delete them as we have the tar and zstd files.
                    if (Files.isDirectory(dayDir)) {
                        try (Stream<Path> paths = Files.walk(dayDir)) {
                            paths.sorted(Comparator.reverseOrder()).forEach(filePath -> {
                                try {
                                    Files.deleteIfExists(filePath);
                                } catch (IOException ioe) {
                                    System.err.println(
                                            "[download] Failed to delete " + filePath + ": " + ioe.getMessage());
                                }
                            });
                        }
                        System.out.println("[download] cleaned per-day folder " + dayDir);
                    }
                }
            } catch (Exception e) {
                System.err.println("[download] Failed to compress tar for day " + dayKey + ": " + e.getMessage());
            }
        }

        /**
         * Download and place all files for the given batch. Returns the highest block number
         * that was successfully downloaded and placed, or -1 if none succeeded.
         */
        long downloadBatch(String dayKey, List<BlockDescriptor> batch) {
            if (batch == null || batch.isEmpty()) {
                return -1L;
            }
            try {
                Files.createDirectories(outRoot.resolve(dayKey));
                Files.createDirectories(tmpRoot);
            } catch (IOException e) {
                System.err.println("[download] Failed to create output/tmp dirs: " + e.getMessage());
                return -1L;
            }

            final ExecutorService pool = Executors.newFixedThreadPool(maxConcurrency);
            final List<Future<Long>> futures = new ArrayList<>();
            for (BlockDescriptor d : batch) {
                futures.add(pool.submit(() -> downloadSingle(dayKey, d)));
            }
            pool.shutdown();

            long highest = -1L;
            for (Future<Long> f : futures) {
                try {
                    Long v = f.get();
                    if (v != null && v > highest) {
                        highest = v;
                    }
                } catch (Exception e) {
                    System.err.println("[download] Failed block download: " + e.getMessage());
                }
            }
            try {
                // Best-effort shutdown; don't block forever
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                pool.shutdownNow();
            }
            return highest;
        }

        /**
         * Download a single block file described by the descriptor, validate, and atomically
         * move it into the per-day folder. On success, returns the block number; on failure,
         * logs and returns -1.
         *
         * "download2" fetcher and hash validation logic. The steps should be:
         *  1. Use the descriptor.filename (and sourceBucket) to locate the object
         *  2. Stream to a temp file under tmpRoot
         *  3. Validate hashes using the shared record-file / block validation utilities
         *  4. Atomically move the temp file into the final day folder
         */
        private long downloadSingle(String dayKey, BlockDescriptor d) {
            Path tmpFile = null;
            try {
                final Path dayDir = outRoot.resolve(dayKey);
                Files.createDirectories(dayDir);

                final String safeName = d.filename != null ? d.filename : ("block-" + d.blockNumber + ".rcd");
                tmpFile = tmpRoot.resolve(dayKey + "-" + safeName + ".part");
                final Path targetFile = dayDir.resolve(safeName);

                if (tmpFile.getParent() != null) {
                    Files.createDirectories(tmpFile.getParent());
                }

                // Interpret sourceRoot as a local/mounted filesystem root where mirror files are present.
                // The mirror-provided filename is taken as a relative path from this root.
                final Path sourceRootPath = Path.of(sourceRoot);
                final String relativeName = d.filename != null ? d.filename : safeName;
                final Path sourceFile = sourceRootPath.resolve(relativeName);

                if (!Files.isRegularFile(sourceFile)) {
                    System.err.println("[download] Source file does not exist or is not a regular file: " + sourceFile);
                    return -1L;
                }

                System.out.println("[download] copying " + sourceFile + " -> " + tmpFile);

                // Copy from source store to temp file in our tmp root.
                Files.copy(sourceFile, tmpFile, StandardCopyOption.REPLACE_EXISTING);

                // Read the bytes for validation; if gzipped, unzip in memory first.
                byte[] fileBytes = Files.readAllBytes(tmpFile);
                byte[] recordBytes = fileBytes;

                if (safeName.endsWith(".gz")) {
                    try {
                        recordBytes = Gzip.ungzipInMemory(fileBytes);
                    } catch (Exception ex) {
                        System.err.println(
                                "[download] Failed to decompress .gz for " + safeName + ": " + ex.getMessage());
                        Files.deleteIfExists(tmpFile);
                        return -1L;
                    }
                }

                // Parse the record file and compute the block hash (download2-style).
                RecordFileInfo recordFileInfo;
                try {
                    recordFileInfo = RecordFileInfo.parse(recordBytes);
                } catch (Exception ex) {
                    System.err.println(
                            "[download] Failed to parse record file for " + safeName + ": " + ex.getMessage());
                    Files.deleteIfExists(tmpFile);
                    return -1L;
                }

                byte[] computedHash = recordFileInfo.blockHash().toByteArray();
                byte[] expectedHash = parseExpectedHash(d.expectedHash);

                if (expectedHash != null) {
                    if (!Arrays.equals(expectedHash, computedHash)) {
                        String expHex = HexFormat.of().formatHex(expectedHash);
                        String gotHex = HexFormat.of().formatHex(computedHash);
                        String expShort = expHex.substring(0, Math.min(8, expHex.length()));
                        String gotShort = gotHex.substring(0, Math.min(8, gotHex.length()));
                        System.err.println("[download] ERROR: Hash mismatch for block " + d.blockNumber + " file "
                                + safeName + " expected=" + expShort + " got=" + gotShort);
                        Files.deleteIfExists(tmpFile);
                        return -1L;
                    }
                }
                // If expectedHash is null, we still parsed the record and computed a hash,
                // exercising the same validation path without external comparison.

                // Atomically move from temp to final location. On same filesystem this will be a rename.
                Files.move(tmpFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                System.out.println("[download] placed " + targetFile + " (block=" + d.blockNumber + ")");

                // Append the successfully validated file into the per-day tar archive.
                appendToDayTar(dayKey, safeName);

                return d.blockNumber;
            } catch (Exception e) {
                System.err.println("[download] Failed to move block " + d.blockNumber + ": " + e.getMessage());
                if (tmpFile != null) {
                    try {
                        Files.deleteIfExists(tmpFile);
                    } catch (IOException ignore) {
                        // ignore
                    }
                }
                return -1L;
            }
        }

        /**
         * Parse a hex-encoded expected hash, allowing an optional 0x prefix.
         * Returns null if the input is null/blank or cannot be parsed.
         */
        private static byte[] parseExpectedHash(String hash) {
            if (hash == null || hash.isBlank()) {
                return null;
            }
            String h = hash.trim();
            if (h.startsWith("0x") || h.startsWith("0X")) {
                h = h.substring(2);
            }
            try {
                return HexFormat.of().parseHex(h);
            } catch (IllegalArgumentException iae) {
                System.err.println(
                        "[download] Warning: Could not parse expected hash '" + hash + "': " + iae.getMessage());
                return null;
            }
        }
    }

    /**
     * Minimal descriptor used by the poller; will align with real mirror schema later.
     */
    static final class BlockDescriptor {
        final long blockNumber;
        final String filename;
        final String timestampIso;
        final String expectedHash;

        BlockDescriptor(long blockNumber, String filename, String timestampIso, String expectedHash) {
            this.blockNumber = blockNumber;
            this.filename = filename;
            this.timestampIso = timestampIso;
            this.expectedHash = expectedHash;
        }

        @Override
        public String toString() {
            return "BlockDescriptor{number=" + blockNumber + ", file='" + filename + "', ts='" + timestampIso + "'}";
        }
    }
}
