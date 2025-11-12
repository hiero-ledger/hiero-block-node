package org.hiero.block.tools.commands.days.subcommands;

import org.hiero.block.tools.commands.mirrornode.MirrorNodeBlockQueryOrder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.hiero.block.tools.commands.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.commands.mirrornode.BlockInfo;
import java.time.LocalDate;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CLI skeleton for the "download-live" command.
 * This version only parses and validates arguments, then prints
 * a structured "not implemented yet" message with the parsed config.
 *
 * Business logic (polling mirror, downloading, validation, rollover, compression)
 * will be added in subsequent stories.
 * This change adds a day-scoped LivePoller skeleton and a MirrorPoller interface; HTTP integration will be added in a subsequent story.
 */
@Command(
        name = "download-live",
        description = "Continuously follow mirror node for new block files; dedupe, validate, and organize into daily folders.",
        mixinStandardHelpOptions = true,
        version = "download-live 0.1"
)
public class DownloadLive implements Runnable {

    @Option(names = "--out",
            required = true,
            paramLabel = "DIR",
            description = "Output directory for daily folders (e.g., /data/records)")
    private Path out;

    @Option(names = "--source-bucket",
            required = true,
            paramLabel = "S3URI",
            description = "Object storage location for record streams (e.g., s3://bucket/recordstreams/)")
    private String sourceBucket;

    @Option(names = "--poll-interval",
            defaultValue = "60s",
            paramLabel = "DURATION",
            description = "Polling interval for mirror API (e.g., 60s, 2m). Parsed later by implementation.")
    private String pollInterval;

    @Option(names = "--batch-size",
            defaultValue = "100",
            paramLabel = "N",
            description = "Max number of block descriptors to request per poll (mirror max is typically 100).")
    private int batchSize;

    @Option(names = "--day-rollover-tz",
            defaultValue = "UTC",
            paramLabel = "TZ",
            description = "Timezone ID used to determine end-of-day rollover (e.g., UTC, America/Los_Angeles).")
    private String dayRolloverTz;

    @Option(names = "--max-concurrency",
            defaultValue = "8",
            paramLabel = "N",
            description = "Max parallel downloads.")
    private int maxConcurrency;

    @Option(names = "--run-poller",
            defaultValue = "false",
            description = "If true, run the day-scoped live poller skeleton (uses a no-op MirrorPoller until HTTP is implemented).")
    private boolean runPoller;

    @Option(names = "--state-json",
            defaultValue = "./state/download-live.json",
            paramLabel = "FILE",
            description = "Path to a small JSON file used to persist last-seen state for resume.")
    private Path stateJsonPath;

    @Override
    public void run() {
        // Skeleton behavior: print parsed configuration and exit.
        System.out.println("[download-live] Starting (skeleton mode)");
        System.out.println("Configuration:");
        System.out.println("  out=" + out);
        System.out.println("  sourceBucket=" + sourceBucket);
        System.out.println("  pollInterval=" + pollInterval);
        System.out.println("  batchSize=" + batchSize);
        System.out.println("  dayRolloverTz=" + dayRolloverTz);
        System.out.println("  maxConcurrency=" + maxConcurrency);
        System.out.println("  runPoller=" + runPoller);
        System.out.println("  stateJsonPath=" + stateJsonPath);

        if (!runPoller) {
            System.out.println("Status: Skeleton ready (use --run-poller to start the day-scoped poll loop)");
            return;
        }

        // --- Start day-scoped poller skeleton ---
        final ZoneId tz = ZoneId.of(dayRolloverTz);
        final Duration interval = parseHumanDuration(pollInterval);
        final LivePoller poller = new LivePoller(interval, tz, batchSize, stateJsonPath);
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

    // --- Helpers ---

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
            throw new CommandLine.ParameterException(new CommandLine(new DownloadLive()),
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
            Files.createDirectories(path.getParent());
            String json = "{\n" +
                    "  \"dayKey\": \"" + st.dayKey + "\",\n" +
                    "  \"lastSeenBlock\": " + st.lastSeenBlock + "\n" +
                    "}\n";
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

    // --- Live poller skeleton ---

    /**
     * A minimal day-scoped live poller that asks the MirrorPoller for new descriptors
     * and logs its activity. For now, it runs a single tick; future stories will
     * extend this into a continuous loop and integrate HTTP.
     */
    static final class LivePoller {
        private final Duration interval;
        private final ZoneId tz;
        private final int batchSize;
        private long lastSeenBlock = -1L;
        private final Path statePath;
        private boolean stateLoadedForToday = false;

        LivePoller(Duration interval, ZoneId tz, int batchSize, Path statePath) {
            this.interval = interval;
            this.tz = tz;
            this.batchSize = batchSize;
            this.statePath = statePath;
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
            System.out.println("[poller] dayKey=" + dayKey + " interval=" + interval + " batchSize=" + batchSize + " lastSeen=" + lastSeenBlock);

            // Fetch latest blocks (descending) and filter to the current day + unseen.
            final List<BlockInfo> latest = FetchBlockQuery.getLatestBlocks(batchSize, MirrorNodeBlockQueryOrder.DESC);
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

            // Sort asc and update lastSeen
            batch.sort((a, b) -> Long.compare(a.blockNumber, b.blockNumber));
            System.out.println("[poller] descriptors=" + batch.size());
            if (!batch.isEmpty()) {
                lastSeenBlock = Math.max(lastSeenBlock, batch.get(batch.size() - 1).blockNumber);
                batch.stream().limit(3).forEach(d ->
                        System.out.println("[poller] sample -> block=" + d.blockNumber + " file=" + d.filename + " ts=" + d.timestampIso));
                // Persist state for resume
                writeState(statePath, new State(dayKey, lastSeenBlock));
            } else {
                System.out.println("[poller] No new blocks this tick.");
            }
        }

        void runContinuouslyForToday() {
            final String currentDayKey = ZonedDateTime.ofInstant(Instant.now(), tz).toLocalDate().toString();
            while (true) {
                final String dayKey = ZonedDateTime.ofInstant(Instant.now(), tz).toLocalDate().toString();
                if (!dayKey.equals(currentDayKey)) {
                    System.out.println("[poller] Day changed (" + currentDayKey + " -> " + dayKey + "); stopping.");
                    return;
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
