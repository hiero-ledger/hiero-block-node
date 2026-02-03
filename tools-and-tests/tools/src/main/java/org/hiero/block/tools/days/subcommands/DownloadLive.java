// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.downloadlive.DateUtil.parseHumanDuration;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.block.tools.days.downloadlive.LiveDownloader;
import org.hiero.block.tools.days.downloadlive.LivePoller;
import org.hiero.block.tools.days.downloadlive.ValidationStatus;
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

    @Option(names = "--max-concurrency", defaultValue = "64", paramLabel = "N", description = "Max parallel downloads.")
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
            names = "--running-hash-status-json",
            defaultValue = "./state/downloadLiveRunningHash.json",
            paramLabel = "FILE",
            description = "Path to a JSON file used to persist the latest validated running hash, "
                    + "in the same shape as validateCmdStatus.json.")
    private Path runningHashStatusPath;

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

    private static final Pattern P_V_DAY = Pattern.compile("\"dayDate\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern P_V_TIME = Pattern.compile("\"recordFileTime\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern P_V_HASH = Pattern.compile("\"endRunningHashHex\"\\s*:\\s*\"([^\"]+)\"");

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
        System.out.println("  runningHashStatusPath=" + runningHashStatusPath);
        System.out.println("  tmpDir=" + tmpDir);
        System.out.println("  addressBookPath=" + addressBookPath);

        if (!runPoller) {
            System.out.println("Status: Ready (use --run-poller to start the day-scoped poll loop)");
            return;
        }

        final ZoneId tz = ZoneId.of("UTC");
        final Duration interval = parseHumanDuration(pollInterval);
        // Load any previously persisted running-hash status so we can resume validation across restarts.
        final ValidationStatus existingValidationStatus = readValidationStatus(runningHashStatusPath);
        final byte[] initialRunningHash =
                (existingValidationStatus != null && existingValidationStatus.endRunningHashHex() != null)
                        ? fromHex(existingValidationStatus.endRunningHashHex())
                        : null;

        final LiveDownloader downloader = new LiveDownloader(
                listingDir,
                downloadedDaysDir,
                tmpDir,
                maxConcurrency,
                addressBookPath,
                runningHashStatusPath,
                initialRunningHash);

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

        final LivePoller poller = new LivePoller(
                interval,
                tz,
                batchSize,
                stateJsonPath,
                runningHashStatusPath,
                downloader,
                startDayParsed,
                endDayParsed);

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

    public static String toHex(byte[] data) {
        if (data == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static byte[] fromHex(String hex) {
        if (hex == null || hex.isEmpty()) {
            return null;
        }
        int len = hex.length();
        if ((len & 1) != 0) {
            throw new IllegalArgumentException("Hex string must have even length");
        }
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            int hi = Character.digit(hex.charAt(i), 16);
            int lo = Character.digit(hex.charAt(i + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("Invalid hex character in: " + hex);
            }
            out[i / 2] = (byte) ((hi << 4) + lo);
        }
        return out;
    }

    private static ValidationStatus readValidationStatus(Path path) {
        try {
            if (path == null || !Files.exists(path)) {
                return null;
            }
            String s = Files.readString(path, StandardCharsets.UTF_8);
            Matcher mDay = P_V_DAY.matcher(s);
            Matcher mTime = P_V_TIME.matcher(s);
            Matcher mHash = P_V_HASH.matcher(s);
            String dayDate = mDay.find() ? mDay.group(1) : null;
            String recordFileTime = mTime.find() ? mTime.group(1) : null;
            String endRunningHashHex = mHash.find() ? mHash.group(1) : null;
            if (dayDate == null || recordFileTime == null || endRunningHashHex == null) {
                return null;
            }
            return new ValidationStatus(dayDate, recordFileTime, endRunningHashHex);
        } catch (Exception e) {
            System.err.println("[poller] Failed to read running-hash status: " + e.getMessage());
            return null;
        }
    }
}
