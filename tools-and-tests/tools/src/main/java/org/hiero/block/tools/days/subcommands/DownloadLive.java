// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.download.DownloadConstants.GCP_PROJECT_ID;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.mirrornode.BlockInfo;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.mirrornode.MirrorNodeBlockQueryOrder;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlockV6;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManagerVirtualThreads;
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
 * <h2>1. Start + end date (finite historical range)</h2>
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
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored")
    private File listingDir = new File("listingsByDay");

    @Option(
            names = {"-d", "--downloaded-days-dir"},
            description = "Directory where downloaded days are stored")
    private File downloadedDaysDir = new File("compressedDays");

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
    private File tmpDir = new File("tmp");

    @Option(
            names = "--address-book",
            paramLabel = "FILE",
            description =
                    "Optional path to an address book file used for future signature validation (e.g., nodeAddressBook.bin).")
    private Path addressBookPath;

    @Override
    public void run() {
        System.out.println("[download-live] Starting");
        System.out.println("Configuration:");
        System.out.println("  listingDir=" + listingDir);
        System.out.println("  downloadedDaysDir=" + downloadedDaysDir);
        System.out.println("  pollInterval=" + pollInterval);
        System.out.println("  batchSize=" + batchSize);
        System.out.println("  startDay=" + startDay);
        System.out.println("  endDay=" + endDay);
        System.out.println("  maxConcurrency=" + maxConcurrency);
        System.out.println("  runPoller=" + runPoller);
        System.out.println("  stateJsonPath=" + stateJsonPath);
        System.out.println("  tmpDir=" + tmpDir);
        System.out.println("  addressBookPath=" + addressBookPath);

        if (!runPoller) {
            System.out.println("Status: Ready (use --run-poller to start the day-scoped poll loop)");
            return;
        }

        // --- Start day-scoped poller with live downloader ---
        final ZoneId tz = ZoneId.of("UTC");
        final Duration interval = parseHumanDuration(pollInterval);
        final LiveDownloader downloader =
                new LiveDownloader(listingDir, downloadedDaysDir, tmpDir, maxConcurrency, addressBookPath);

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
                    "Invalid --start-day/--end-day; expected format YYYY-MM-DD. startDay=" + startDay + " endDay="
                            + endDay);
        }

        final LivePoller poller =
                new LivePoller(interval, tz, batchSize, stateJsonPath, downloader, startDayParsed, endDayParsed);

        // Ensure we release GCS and executor resources on JVM shutdown.
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            System.out.println(
                                    "[download-live] Shutdown requested; closing LiveDownloader resources...");
                            try {
                                downloader.shutdown();
                            } catch (Exception e) {
                                System.err.println(
                                        "[download-live] Error while shutting down downloader: " + e.getMessage());
                            }
                        },
                        "download-live-shutdown"));

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

        void runOnceForDay(LocalDate day) {
            final ZonedDateTime start = day.atStartOfDay(ZoneId.of("UTC"));
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

            final LocalDate lowerBoundDay = (configuredStartDay != null) ? configuredStartDay : day;
            final long startSeconds =
                    lowerBoundDay.atStartOfDay(ZoneId.of("UTC")).toEpochSecond();

            final List<String> timestampFilters = new ArrayList<>();
            timestampFilters.add("gte:" + startSeconds + ".000000000");

            if (configuredEndDay != null) {
                final long endSeconds = configuredEndDay
                        .plusDays(1)
                        .atStartOfDay(ZoneId.of("UTC"))
                        .toEpochSecond();
                timestampFilters.add("lt:" + endSeconds + ".000000000");
            }

            final List<BlockInfo> latest =
                    FetchBlockQuery.getLatestBlocks(batchSize, MirrorNodeBlockQueryOrder.DESC, timestampFilters);
            System.out.println("[poller] Latest blocks size: " + latest.size());

            final List<LiveDownloader.BlockDescriptor> batch =
                    latest.stream()
                            .filter(b -> lastSeenBlock < 0 || b.number > lastSeenBlock)
                            .map(b -> {
                                Instant ts = parseMirrorTimestamp(
                                        b.timestampFrom != null ? b.timestampFrom : b.timestampTo);
                                return new Object[] {b, ts};
                            })
                            .filter(arr -> arr[1] != null)
                            .map(arr -> {
                                BlockInfo b = (BlockInfo) arr[0];
                                Instant ts = (Instant) arr[1];
                                ZonedDateTime zts = ZonedDateTime.ofInstant(ts, ZoneId.of("UTC"));
                                if (zts.isBefore(start) || !zts.isBefore(end)) {
                                    return null;
                                }
                                return new LiveDownloader.BlockDescriptor(b.number, b.name, ts.toString(), b.hash);
                            })
                            .filter(Objects::nonNull)
                            .sorted(Comparator.comparingLong(d -> d.blockNumber))
                            .toList();
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
                writeState(statePath, new State(dayKey, lastSeenBlock));
            } else {
                System.out.println("[poller] No new blocks this tick for " + dayKey + ".");
            }
        }

        void runContinuouslyForToday() {
            LocalDate todayInTz =
                    ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).toLocalDate();
            LocalDate logicalDay = (configuredStartDay != null && !configuredStartDay.isAfter(todayInTz))
                    ? configuredStartDay
                    : todayInTz;

            while (true) {
                if (configuredEndDay != null && logicalDay.isAfter(configuredEndDay)) {
                    System.out.println(
                            "[poller] Reached configured end day " + configuredEndDay + "; exiting live poller.");
                    return;
                }

                LocalDate currentToday =
                        ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).toLocalDate();

                if (logicalDay.isBefore(currentToday)) {
                    final String dayKey = logicalDay.toString();
                    System.out.println("[poller] Processing historical day " + dayKey + " (before current live day "
                            + currentToday + ")");
                    stateLoadedForToday = false;
                    runOnceForDay(logicalDay);
                    try {
                        System.out.println("[poller] Finalizing historical day " + dayKey);
                        downloader.finalizeDay(dayKey);
                    } catch (Exception e) {
                        System.err.println(
                                "[poller] Failed to finalize day archive for " + dayKey + ": " + e.getMessage());
                    }
                    logicalDay = logicalDay.plusDays(1);
                    continue;
                }

                final LocalDate liveDay = currentToday;
                final String liveDayKey = liveDay.toString();

                stateLoadedForToday = false;
                runOnceForDay(liveDay);

                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }

                final LocalDate afterSleepToday =
                        ZonedDateTime.ofInstant(Instant.now(), tz).toLocalDate();
                if (!afterSleepToday.equals(liveDay)) {
                    System.out.println("[poller] Day changed (" + liveDayKey + " -> " + afterSleepToday
                            + "); finalizing previous day and rolling over.");
                    try {
                        downloader.finalizeDay(liveDayKey);
                    } catch (Exception e) {
                        System.err.println(
                                "[poller] Failed to finalize day archive for " + liveDayKey + ": " + e.getMessage());
                    }
                }

                logicalDay = afterSleepToday;
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
        private final File listingDir;
        private final File downloadedDaysDir;
        private final File tmpRoot;
        private final int maxConcurrency;
        private final Path addressBookPath;
        private final org.hiero.block.tools.days.model.AddressBookRegistry addressBookRegistry;
        private final ConcurrentDownloadManagerVirtualThreads downloadManager;
        // Running previous record-file hash used to validate the block hash chain across files.
        private byte[] previousRecordFileHash;
        // Single-threaded executor used for background compression of per-day tar files.
        private final ExecutorService compressionExecutor;

        LiveDownloader(
                File listingDir, File downloadedDaysDir, File tmpRoot, int maxConcurrency, Path addressBookPath) {
            this.listingDir = listingDir;
            this.downloadedDaysDir = downloadedDaysDir;
            this.tmpRoot = tmpRoot;
            this.maxConcurrency = Math.max(1, maxConcurrency);
            this.addressBookPath = addressBookPath;
            // Initialise the address book registry:
            //  - If a JSON history file is provided via --address-book, load from it
            //  - Otherwise fall back to the built-in Genesis address book
            if (addressBookPath != null) {
                System.out.println("[download] Loading address book from " + addressBookPath);
                this.addressBookRegistry = new AddressBookRegistry(addressBookPath);
            } else {
                System.out.println("[download] No --address-book supplied; using Genesis address book.");
                this.addressBookRegistry = new AddressBookRegistry();
            }
            // GCS-only: use ConcurrentDownloadManagerTransferManager to fetch objects from the configured
            // bucket/prefix.
            // NOTE: if the actual ConcurrentDownloadManagerTransferManager constructor has a different signature,
            // adjust this
            // call to match its configuration factory used by the historic download2 tooling.
            Storage storage = StorageOptions.grpc()
                    .setAttemptDirectPath(false)
                    .setProjectId(GCP_PROJECT_ID)
                    .build()
                    .getService();
            this.downloadManager =
                    ConcurrentDownloadManagerVirtualThreads.newBuilder(storage).build();
            this.compressionExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "download-live-compress");
                t.setDaemon(true);
                return t;
            });
        }

        /**
         * Perform full block validation (record + signatures + sidecars) using the same
         * RecordFileBlockV6 pipeline as the offline Validate tool, but using the files
         * already downloaded for this block (no extra GCS fetches).
         *
         * @param startRunningHash the running hash from the previous block/file, may be null for the first block
         * @param recordFileTime   the record file time derived from mirror node timestamp
         * @param result           the block download result containing all in-memory files for this block
         * @param tmpFile          optional temp file path used only for quarantine bookkeeping (may be null)
         * @return true if full validation succeeds, false otherwise
         */
        private boolean fullBlockValidate(
                byte[] startRunningHash,
                Instant recordFileTime,
                DownloadDayLiveImpl.BlockDownloadResult result,
                Path tmpFile) {
            try {
                InMemoryFile primaryRecord = result.files.stream()
                        .filter(f -> {
                            final String name = f.path().getFileName().toString();
                            return name.endsWith(".rcd") && !name.contains("_node_");
                        })
                        .findFirst()
                        .orElse(null);

                List<InMemoryFile> signatures = result.files.stream()
                        .filter(f -> f.path().getFileName().toString().endsWith(".rcd_sig"))
                        .toList();

                List<InMemoryFile> sidecars = result.files.stream()
                        .filter(f -> f.path().getFileName().toString().endsWith(".rcd_sc"))
                        .toList();

                if (primaryRecord == null) {
                    System.err.println("[download] No primary record found for block " + result.blockNumber
                            + "; skipping full validation.");
                    return false;
                }

                final UnparsedRecordBlockV6 block = new UnparsedRecordBlockV6(
                        recordFileTime,
                        primaryRecord,
                        List.of(), // no "other" records in live mode
                        signatures,
                        sidecars,
                        List.of()); // no "other" sidecars in live mode

                final UnparsedRecordBlockV6.ValidationResult vr =
                        block.validate(startRunningHash, addressBookRegistry.getCurrentAddressBook());

                if (!vr.isValid()) {
                    System.err.println("[download] Full block validation failed for block " + result.blockNumber + ": "
                            + vr.warningMessages());
                    // tmpFile may be null; quarantine() is a no-op in that case.
                    quarantine(
                            tmpFile,
                            primaryRecord.path().getFileName().toString(),
                            result.blockNumber,
                            "full block validation failure");
                    return false;
                }

                final String addressBookChanges =
                        addressBookRegistry.updateAddressBook(block.recordFileTime(), vr.addressBookTransactions());
                if (addressBookChanges != null && !addressBookChanges.isBlank()) {
                    System.out.println("[download] " + addressBookChanges);
                }

                return true;
            } catch (Exception ex) {
                System.err.println("[download] Exception during full block validation for block " + result.blockNumber
                        + ": " + ex.getMessage());
                quarantine(tmpFile, "unknown", result.blockNumber, "exception during full block validation");
                return false;
            }
        }

        /**
         * Shutdown hook for releasing GCS transfer resources and the background compression executor.
         * This is invoked from the top-level DownloadLive command via a JVM shutdown hook.
         */
        void shutdown() {
            // Close the GCS transfer manager if it supports AutoCloseable/Closeable.
            try {
                if (downloadManager instanceof AutoCloseable closeable) {
                    closeable.close();
                }
            } catch (Exception e) {
                System.err.println("[download] Failed to close download manager: " + e.getMessage());
            }

            // Stop accepting new compression tasks and attempt a graceful shutdown.
            compressionExecutor.shutdown();
            try {
                if (!compressionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    compressionExecutor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                compressionExecutor.shutdownNow();
            }
        }

        /**
         * Move a problematic temp file into a quarantine area under {@code outRoot} so that it can be
         * inspected later instead of being silently discarded.
         *
         * This is used when validation fails (e.g., block hash mismatch with mirror expectedHash).
         */
        private void quarantine(Path tmpFile, String safeName, long blockNumber, String reason) {
            if (tmpFile == null) {
                return;
            }
            try {
                final Path quarantineDir = tmpFile.resolve("quarantine");
                Files.createDirectories(quarantineDir);
                final String targetName = blockNumber + "-" + safeName;
                final Path targetFile = quarantineDir.resolve(targetName);
                Files.move(tmpFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                System.err.println("[download] Quarantined file for block " + blockNumber + " (" + safeName + ") -> "
                        + targetFile + " reason=" + reason);
            } catch (IOException ioe) {
                System.err.println("[download] Failed to quarantine file for block " + blockNumber + " (" + safeName
                        + "): " + ioe.getMessage());
            }
        }

        /**
         * Append the given file (by entryName) to the per-day tar archive using the system tar command.
         * The tar file is created under outRoot as dayKey.tar and entries are taken from the per-day folder.
         */
        void appendToDayTar(String dayKey, String entryName) {
            try {
                final Path dayDir = downloadedDaysDir.toPath().resolve(dayKey);
                final Path tarPath = downloadedDaysDir.toPath().resolve(dayKey + ".tar");

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
         * then compresses dayKey.tar into dayKey.tar.zstd and cleans up the per-day folder.
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
                final Path tarPath = downloadedDaysDir.toPath().resolve(dayKey + ".tar");
                final Path dayDir = downloadedDaysDir.toPath().resolve(dayKey);
                if (!Files.isRegularFile(tarPath)) {
                    System.out.println("[download] No tar file for day " + dayKey + " to compress; skipping.");
                    return;
                }
                final Path zstdPath = downloadedDaysDir.toPath().resolve(dayKey + ".tar.zstd");
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
                Files.createDirectories(downloadedDaysDir.toPath().resolve(dayKey));
                Files.createDirectories(tmpRoot.toPath());
            } catch (IOException e) {
                System.err.println("[download] Failed to create output/tmp dirs: " + e.getMessage());
                return -1L;
            }

            // Ensure deterministic order
            batch.sort(Comparator.comparingLong(b -> b.blockNumber));

            final LocalDate day = LocalDate.parse(dayKey);
            final LocalDate todayUtc = LocalDate.now(ZoneId.of("UTC"));

            // Historical days (strictly before today) use the full day-based pipeline.
            if (day.isBefore(todayUtc)) {
                final BlockDescriptor lastDescriptor = batch.get(batch.size() - 1);
                final long result = downloadSingle(dayKey, lastDescriptor);
                if (result < 0L) {
                    // If the day-level download failed, report no progress for this tick.
                    return -1L;
                }
                // On success, we treat the highest block number in this batch as the new processed height.
                return lastDescriptor.blockNumber;
            }

            // Live day (today or later): process each new block incrementally using the same
            // listing-based selection and MD5/ungzip rules as the historic downloadDay tooling.
            final Map<LocalDate, DayBlockInfo> daysInfo = loadDayBlockInfoMap();
            final DayBlockInfo dayBlockInfo = daysInfo.get(day);
            if (dayBlockInfo == null) {
                System.err.println("[download] No DayBlockInfo found for live dayKey=" + dayKey + "; skipping batch of "
                        + batch.size() + " blocks.");
                return -1L;
            }

            try {

                final BlockTimeReader blockTimeReader = new BlockTimeReader();
                final Path dayDir = downloadedDaysDir.toPath().resolve(dayKey);
                long highest = -1L;

                for (BlockDescriptor d : batch) {
                    try {
                        final DownloadDayLiveImpl.BlockDownloadResult result =
                                DownloadDayLiveImpl.downloadSingleBlockForLive(
                                        downloadManager,
                                        dayBlockInfo,
                                        blockTimeReader,
                                        listingDir.toPath(),
                                        day.getYear(),
                                        day.getMonthValue(),
                                        day.getDayOfMonth(),
                                        d.blockNumber,
                                        previousRecordFileHash);

                        // Perform full block validation (record + signatures + sidecars) using the
                        // files already downloaded for this block.
                        final Instant recordFileTime = Instant.parse(d.timestampIso);
                        final boolean fullyValid = fullBlockValidate(
                                previousRecordFileHash,
                                recordFileTime,
                                result,
                                null); // no temp file for now; quarantine will no-op on null

                        if (!fullyValid) {
                            System.err.println("[download] Skipping persistence for block " + d.blockNumber
                                    + " due to full block validation failure.");
                            continue;
                        }

                        // Persist each fully validated file into the per-day folder and append it into the per-day tar.
                        for (InMemoryFile file : result.files) {
                            final Path relativePath = file.path();
                            final Path targetPath = dayDir.resolve(relativePath);
                            if (targetPath.getParent() != null) {
                                Files.createDirectories(targetPath.getParent());
                            }
                            Files.write(targetPath, file.data());

                            final String entryName =
                                    dayDir.relativize(targetPath).toString();
                            appendToDayTar(dayKey, entryName);
                        }

                        // Update running hash and highest processed block only after full validation and persistence.
                        previousRecordFileHash = result.newPreviousRecordFileHash;
                        highest = d.blockNumber;
                    } catch (Exception e) {
                        System.err.println("[download] Failed live block download for block " + d.blockNumber + ": "
                                + e.getMessage());
                    }
                }

                return highest;
            } catch (Exception e) {
                System.err.println("[download] Failed to download day " + dayKey + ": " + e.getMessage());
                e.printStackTrace();
            }
            return -1L;
        }

        private long downloadSingle(String dayKey, BlockDescriptor d) {
            // Use the logical dayKey from the poller rather than recomputing from the block timestamp.
            // We only need the DayBlockInfo for this single day, not a full date range.
            try {
                BlockTimeReader blockTimeReader = new BlockTimeReader();
                final LocalDate day = LocalDate.parse(dayKey); // dayKey is YYYY-MM-DD in the rollover timezone
                final Map<LocalDate, DayBlockInfo> daysInfo = loadDayBlockInfoMap();
                final DayBlockInfo dayBlockInfo = daysInfo.get(day);
                if (dayBlockInfo == null) {
                    System.err.println("[download] No DayBlockInfo found for dayKey=" + dayKey + "; skipping block "
                            + d.blockNumber);
                    return -1L;
                }
                final long totalDays = 1L;
                final long overallStartMillis = System.currentTimeMillis();
                LocalDate localDate = day;
                try {
                    previousRecordFileHash = DownloadDayLiveImpl.downloadDay(
                            downloadManager,
                            dayBlockInfo,
                            blockTimeReader,
                            listingDir.toPath(),
                            downloadedDaysDir.toPath(),
                            localDate.getYear(),
                            localDate.getMonthValue(),
                            localDate.getDayOfMonth(),
                            previousRecordFileHash,
                            totalDays,
                            0, // progressStart as day index (0-based)
                            overallStartMillis);

                    System.out.println("[download] block " + d.blockNumber + " downloaded " + totalDays + " days"
                            + " previousRecordFileHash:" + previousRecordFileHash);

                    return d.blockNumber;
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return -1L;
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
                return "BlockDescriptor{number=" + blockNumber + ", file='" + filename + "', ts='" + timestampIso
                        + "'}";
            }
        }
    }
}
