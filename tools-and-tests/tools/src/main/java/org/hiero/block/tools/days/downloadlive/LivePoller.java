// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.hiero.block.tools.days.downloadlive.DateUtil.parseMirrorTimestamp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.hiero.block.tools.mirrornode.BlockInfo;
import org.hiero.block.tools.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.mirrornode.MirrorNodeBlockQueryOrder;
import org.hiero.block.tools.utils.PrettyPrint;

/**
 * Day-scoped live poller that queries the mirror node for latest blocks,
 * filters to the current day + unseen blocks, then delegates to the
 * LiveDownloader to fetch and place files.
 */
public class LivePoller {

    // Historical-day completion: only finalize once we observe blocks from the next day.
    // We extend the mirror query window slightly past day-end so we can detect rollover.
    private static final Duration HISTORICAL_ROLLOVER_LOOKAHEAD = Duration.ofHours(2);

    private final Duration interval;
    private final ZoneId tz;
    private final int batchSize;
    private long lastSeenBlock = -1L;
    private Instant lastSeenTimestamp = null;
    // If we have a lastSeenBlock but no timestamp in state (older state files), we "seek" forward through
    // the day in coarse steps until mirror results include blocks beyond lastSeenBlock.
    private Instant seekWindowStart = null;
    private final Path statePath;
    private final Path runningHashStatusPath;
    private final LiveDownloader downloader;
    private boolean stateLoadedForToday = false;
    // Optional global date range for ingestion.
    private final LocalDate configuredStartDay;
    private final LocalDate configuredEndDay;

    private static final Gson GSON = new GsonBuilder().create();

    /**
     * Best-effort read of the running-hash status file. This file is written by other parts of the
     * toolchain and may be absent or partially populated.
     */
    private static RunningHashStatus readRunningHashStatus(final Path path) {
        try {
            if (path == null || !Files.exists(path)) {
                return null;
            }
            final String raw = Files.readString(path, StandardCharsets.UTF_8);
            final JsonObject obj = JsonParser.parseString(raw).getAsJsonObject();

            final String dayDate = obj.has("dayDate") && !obj.get("dayDate").isJsonNull()
                    ? obj.get("dayDate").getAsString()
                    : null;

            Instant recordFileTime = null;
            if (obj.has("recordFileTime") && !obj.get("recordFileTime").isJsonNull()) {
                try {
                    recordFileTime = Instant.parse(obj.get("recordFileTime").getAsString());
                } catch (Exception ignored) {
                    recordFileTime = null;
                }
            }

            final String endRunningHashHex = obj.has("endRunningHashHex")
                            && !obj.get("endRunningHashHex").isJsonNull()
                    ? obj.get("endRunningHashHex").getAsString()
                    : null;

            return new RunningHashStatus(dayDate, recordFileTime, endRunningHashHex);
        } catch (Exception e) {
            System.err.println("[poller] Failed to read running hash status: " + e.getMessage());
            return null;
        }
    }

    /**
     * If persisted block state and persisted running-hash state disagree, we must not resume with a
     * mismatched previous-hash cursor.
     *
     * We repair by resetting the running-hash cursor to null so the downloader can re-initialize
     * from a safe source (e.g. mirror node / validation) rather than repeatedly failing with
     * previous-hash mismatches.
     */
    private void repairRunningHashCursorIfInconsistent(final String dayKey) {
        final RunningHashStatus rhs = readRunningHashStatus(runningHashStatusPath);
        if (rhs == null) {
            return;
        }

        // If the file belongs to a different day, reset it.
        if (rhs.dayDate != null && !rhs.dayDate.equals(dayKey)) {
            System.out.println("[poller] Running-hash status dayDate=" + rhs.dayDate + " does not match state dayKey="
                    + dayKey + "; resetting running-hash cursor.");
            writeRunningHashStatusReset(dayKey);
            return;
        }

        // If we have a state timestamp and the running-hash recordFileTime is ahead of it, the two
        // cursors are out of sync (hash cursor advanced beyond block cursor). Reset to avoid
        // previous-hash mismatch loops.
        if (lastSeenTimestamp != null && rhs.recordFileTime != null && rhs.recordFileTime.isAfter(lastSeenTimestamp)) {
            System.out.println("[poller] Running-hash recordFileTime=" + rhs.recordFileTime
                    + " is ahead of state lastSeenTimestamp=" + lastSeenTimestamp
                    + " for dayKey=" + dayKey + "; resetting running-hash cursor.");
            writeRunningHashStatusReset(dayKey);
        }
    }

    private void writeRunningHashStatusReset(final String dayKey) {
        if (runningHashStatusPath == null) {
            return;
        }
        try {
            if (runningHashStatusPath.getParent() != null) {
                Files.createDirectories(runningHashStatusPath.getParent());
            }
            final JsonObject out = new JsonObject();
            out.addProperty("dayDate", dayKey);
            // Null cursor forces downstream components to re-initialize previous hash safely.
            out.add("recordFileTime", null);
            out.add("endRunningHashHex", null);
            Files.writeString(runningHashStatusPath, out.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.err.println("[poller] Failed to reset running hash status: " + e.getMessage());
        }
    }

    private static final class RunningHashStatus {
        final String dayDate;
        final Instant recordFileTime;
        final String endRunningHashHex;

        private RunningHashStatus(final String dayDate, final Instant recordFileTime, final String endRunningHashHex) {
            this.dayDate = dayDate;
            this.recordFileTime = recordFileTime;
            this.endRunningHashHex = endRunningHashHex;
        }
    }

    private static final class PollResult {
        final int processedDescriptors;
        final boolean sawNextDayBlock;

        private PollResult(final int processedDescriptors, final boolean sawNextDayBlock) {
            this.processedDescriptors = processedDescriptors;
            this.sawNextDayBlock = sawNextDayBlock;
        }
    }

    public LivePoller(
            final Duration interval,
            final ZoneId tz,
            final int batchSize,
            final Path statePath,
            final Path runningHashStatusPath,
            final LiveDownloader downloader,
            final LocalDate configuredStartDay,
            final LocalDate configuredEndDay) {
        this.interval = interval;
        this.tz = tz;
        this.batchSize = batchSize;
        this.statePath = statePath;
        this.runningHashStatusPath = runningHashStatusPath;
        this.downloader = downloader;
        this.configuredStartDay = configuredStartDay;
        this.configuredEndDay = configuredEndDay;
    }

    int runOnceForDay(LocalDate day) {
        return runOnceForDayInternal(day, false).processedDescriptors;
    }

    private PollResult runOnceForDayInternal(final LocalDate day, final boolean rolloverProbe) {
        final ZonedDateTime dayStart = day.atStartOfDay(ZoneId.of("UTC"));
        final ZonedDateTime dayEnd = dayStart.plusDays(1);
        final String dayKey = day.toString(); // YYYY-MM-DD

        if (!stateLoadedForToday) {
            State st = readState(statePath);
            if (st != null && dayKey.equals(st.getDayKey())) {
                lastSeenBlock = st.getLastSeenBlock();

                // Resume timestamp cursor if present (epoch millis), otherwise enable seek fallback.
                seekWindowStart = null;
                if (st.getLastSeenTimestamp() >= 0) {
                    lastSeenTimestamp = Instant.ofEpochMilli(st.getLastSeenTimestamp());
                    System.out.println("[poller] Resumed lastSeenBlock from state for day " + dayKey + ": "
                            + lastSeenBlock + ", lastSeenTimestamp=" + lastSeenTimestamp);
                } else {
                    lastSeenTimestamp = null;
                    if (lastSeenBlock >= 0) {
                        seekWindowStart = dayStart.toInstant();
                        System.out.println("[poller] Resumed lastSeenBlock from state for day " + dayKey + ": "
                                + lastSeenBlock + " (no timestamp; enabling seek cursor from day start)");
                    } else {
                        System.out.println(
                                "[poller] Resumed lastSeenBlock from state for day " + dayKey + ": " + lastSeenBlock);
                    }
                }
            } else {
                lastSeenBlock = -1L;
                lastSeenTimestamp = null;
                seekWindowStart = null;
                System.out.println(
                        "[poller] No matching state for " + dayKey + " (starting fresh from beginning of day)."
                                + (rolloverProbe ? " [rollover-probe enabled]" : ""));
            }
            repairRunningHashCursorIfInconsistent(dayKey);
            stateLoadedForToday = true;
        }
        System.out.println("[poller] dayKey=" + dayKey + " interval=" + interval + " batchSize=" + batchSize
                + " lastSeen=" + lastSeenBlock);

        final Instant windowStartInstant;
        if (lastSeenTimestamp != null) {
            windowStartInstant = lastSeenTimestamp.plusNanos(1);
        } else if (seekWindowStart != null) {
            windowStartInstant = seekWindowStart;
        } else {
            windowStartInstant = dayStart.toInstant();
        }
        final Instant baseWindowEndInstant = dayEnd.toInstant();
        final Instant windowEndInstant =
                rolloverProbe ? baseWindowEndInstant.plus(HISTORICAL_ROLLOVER_LOOKAHEAD) : baseWindowEndInstant;

        final long startSeconds = windowStartInstant.getEpochSecond();
        final long endSeconds = windowEndInstant.getEpochSecond();

        final List<String> timestampFilters = new ArrayList<>();
        timestampFilters.add("gte:" + startSeconds + ".000000000");
        timestampFilters.add("lt:" + endSeconds + ".000000000");

        final List<BlockInfo> latest =
                FetchBlockQuery.getLatestBlocks(batchSize, MirrorNodeBlockQueryOrder.ASC, timestampFilters);
        System.out.println("[poller] Latest blocks size: " + latest.size());

        boolean sawNextDay = false;

        final List<BlockDescriptor> batch = latest.stream()
                .filter(b -> lastSeenBlock < 0 || b.number > lastSeenBlock)
                .map(b -> {
                    Instant ts = parseMirrorTimestamp(b.timestampFrom != null ? b.timestampFrom : b.timestampTo);
                    return new Object[] {b, ts};
                })
                .filter(arr -> arr[1] != null)
                .map(arr -> {
                    BlockInfo b = (BlockInfo) arr[0];
                    Instant ts = (Instant) arr[1];
                    ZonedDateTime zts = ZonedDateTime.ofInstant(ts, ZoneId.of("UTC"));

                    // Detect rollover blocks when probing (any block timestamp >= dayEnd belongs to next day).
                    if (rolloverProbe && !zts.isBefore(dayEnd)) {
                        return new Object[] {null, Boolean.TRUE};
                    }

                    // Only process blocks that belong to the target day.
                    if (zts.isBefore(dayStart) || !zts.isBefore(dayEnd)) {
                        return null;
                    }
                    return new Object[] {new BlockDescriptor(b.number, b.name, ts.toString(), b.hash), Boolean.FALSE};
                })
                .filter(Objects::nonNull)
                .peek(obj -> {
                    if (obj[1] == Boolean.TRUE) {
                        // Rollover marker observed.
                        // Note: we do not process this descriptor here; it belongs to the next day.
                    }
                })
                .filter(obj -> obj[1] != Boolean.TRUE)
                .map(obj -> (BlockDescriptor) obj[0])
                .sorted(Comparator.comparingLong(d -> d.blockNumber()))
                .toList();

        // Re-scan to determine if we saw rollover blocks (kept separate to avoid mixing types).
        if (rolloverProbe) {
            for (BlockInfo b : latest) {
                Instant ts = parseMirrorTimestamp(b.timestampFrom != null ? b.timestampFrom : b.timestampTo);
                if (ts == null) {
                    continue;
                }
                ZonedDateTime zts = ZonedDateTime.ofInstant(ts, ZoneId.of("UTC"));
                if (!zts.isBefore(dayEnd)) {
                    sawNextDay = true;
                    break;
                }
            }
        }

        System.out.println("[poller] descriptors=" + batch.size());
        if (!batch.isEmpty()) {
            final long previousLastSeenBlock = lastSeenBlock;

            final long highestDownloaded = downloader.downloadBatch(dayKey, batch);

            // Only advance the timestamp cursor if we actually advanced the block cursor.
            // Otherwise we risk persisting an inconsistent state (old block number + newer timestamp)
            // which causes resume to jump forward in time while the running-hash cursor is still behind.
            if (highestDownloaded > lastSeenBlock) {
                lastSeenBlock = highestDownloaded;

                // Find the descriptor corresponding to the highest downloaded block so the next window
                // starts just after this block.
                final BlockDescriptor highestDescriptor = batch.stream()
                        .filter(d -> d.blockNumber() == highestDownloaded)
                        .findFirst()
                        .orElse(batch.get(batch.size() - 1));

                lastSeenTimestamp = Instant.parse(highestDescriptor.timestampIso());
                seekWindowStart = null;

                // Persist the current state so subsequent runs can resume from the latest known cursor.
                writeState(
                        statePath,
                        new State(
                                dayKey,
                                lastSeenBlock,
                                lastSeenTimestamp != null ? lastSeenTimestamp.toEpochMilli() : -1L));
            } else {
                // No progress; do NOT overwrite state (especially the timestamp) on a failed batch.
                System.out.println("[poller] Batch made no progress for " + dayKey + " (highestDownloaded="
                        + highestDownloaded + ", lastSeenBlock=" + previousLastSeenBlock + "); state not updated.");
            }

            batch.stream()
                    .limit(3)
                    .forEach(d -> System.out.println("[poller] sample -> block=" + d.blockNumber() + " file="
                            + d.filename() + " ts=" + d.timestampIso()));

            return new PollResult(batch.size(), sawNextDay);
        } else {
            System.out.println("[poller] No new blocks this tick for " + dayKey + ".");

            if (seekWindowStart != null) {
                final Instant next = seekWindowStart.plus(Duration.ofHours(1));
                final Instant max = dayEnd.toInstant();
                if (next.isAfter(max)) {
                    seekWindowStart = null;
                    System.out.println(
                            "[poller] Seek cursor reached day end without finding blocks beyond lastSeenBlock; disabling seek.");
                } else {
                    seekWindowStart = next;
                    System.out.println("[poller] Advancing seek cursor for " + dayKey + " to " + seekWindowStart
                            + " (lastSeenBlock=" + lastSeenBlock + ")");
                }
            }

            return new PollResult(0, sawNextDay);
        }
    }

    public void runContinuouslyForToday() {
        LocalDate todayInTz =
                ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).toLocalDate();
        LocalDate logicalDay =
                (configuredStartDay != null && !configuredStartDay.isAfter(todayInTz)) ? configuredStartDay : todayInTz;

        // Track start time and initial day for ETA calculation during historical processing
        final long historicalStartTime = System.currentTimeMillis();
        final LocalDate historicalStartDay = logicalDay;

        while (true) {
            if (configuredEndDay != null && logicalDay.isAfter(configuredEndDay)) {
                PrettyPrint.clearProgress();
                System.out.println(
                        "[poller] Reached configured end day " + configuredEndDay + "; exiting live poller.");
                return;
            }

            LocalDate currentToday =
                    ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).toLocalDate();

            if (logicalDay.isBefore(currentToday)) {
                final String dayKey = logicalDay.toString();

                // Calculate progress for historical day processing
                int dayIndex = (int) ChronoUnit.DAYS.between(historicalStartDay, logicalDay);
                long totalHistoricalDays = ChronoUnit.DAYS.between(historicalStartDay, currentToday);

                System.out.println("[poller] Processing historical day " + dayKey + " (" + (dayIndex + 1) + " of "
                        + totalHistoricalDays + ") using optimized pipelined download");

                // For historical days, use the optimized pipelined download method.
                // This is MUCH faster than the sequential per-block approach because:
                // 1. Downloads are pipelined using LinkedBlockingDeque
                // 2. Day listing files are cached and loaded once per day, not per block
                // 3. DayBlockInfo is cached and reused across all days
                // 4. Writes directly to tar.zstd without intermediate tar file or subprocess
                // 5. BlockTimeReader is reused across batches

                // Get start block number from state if resuming
                State st = readState(statePath);
                long startBlockNumber = 0;
                if (st != null && dayKey.equals(st.getDayKey()) && st.getLastSeenBlock() > 0) {
                    startBlockNumber = st.getLastSeenBlock() + 1;
                    System.out.println(
                            "[poller] Resuming historical day " + dayKey + " from block " + startBlockNumber);
                }

                try {
                    // Use the optimized pipelined download method
                    byte[] finalHash = downloader.downloadDayPipelined(
                            dayKey, startBlockNumber, historicalStartTime, dayIndex, totalHistoricalDays);

                    if (finalHash != null) {
                        PrettyPrint.clearProgress();
                        System.out.println("[poller] Completed historical day " + dayKey);
                    } else {
                        PrettyPrint.clearProgress();
                        System.out.println("[poller] Historical day " + dayKey + " was skipped (already exists)");
                    }

                    // Invalidate day listings cache for next day
                    downloader.invalidateDayListingsCache();

                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.err.println("[poller] Failed to download historical day " + dayKey + ": " + e.getMessage());
                    e.printStackTrace();
                }

                logicalDay = logicalDay.plusDays(1);
                continue;
            }

            PrettyPrint.clearProgress();
            final LocalDate liveDay = currentToday;
            final String liveDayKey = liveDay.toString();

            // For the current live day, refresh metadata/listings on every poll tick
            // before we query mirror node and attempt downloads.
            try {
                downloader.refreshListingsForLiveDay(liveDay);
            } catch (Exception e) {
                System.err.println(
                        "[poller] Failed to refresh metadata listings for live day " + liveDay + ": " + e.getMessage());
            }

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

    public static State readState(final Path path) {
        try {
            if (path == null) {
                return null;
            }
            if (!Files.exists(path)) {
                return null;
            }

            final String raw = Files.readString(path, StandardCharsets.UTF_8);
            final JsonObject obj = JsonParser.parseString(raw).getAsJsonObject();

            final JsonElement dayEl = obj.get("dayKey");
            if (dayEl == null || dayEl.isJsonNull()) {
                return null;
            }
            final String dayKey = dayEl.getAsString();

            final long lastSeenBlock =
                    obj.has("lastSeenBlock") && !obj.get("lastSeenBlock").isJsonNull()
                            ? obj.get("lastSeenBlock").getAsLong()
                            : -1L;

            long lastSeenTsMillis = -1L;
            if (obj.has("lastSeenTimestamp") && !obj.get("lastSeenTimestamp").isJsonNull()) {
                final JsonElement tsEl = obj.get("lastSeenTimestamp");
                // Support legacy/expected format (epoch millis) as well as ISO-8601 strings.
                if (tsEl.isJsonPrimitive() && tsEl.getAsJsonPrimitive().isNumber()) {
                    lastSeenTsMillis = tsEl.getAsLong();
                } else {
                    final String tsStr = tsEl.getAsString();
                    try {
                        lastSeenTsMillis = Instant.parse(tsStr).toEpochMilli();
                    } catch (Exception ignored) {
                        // Keep -1; caller will fall back to seek mode.
                        lastSeenTsMillis = -1L;
                    }
                }
            }

            return new State(dayKey, lastSeenBlock, lastSeenTsMillis);
        } catch (Exception e) {
            System.err.println("[poller] Failed to read state: " + e.getMessage());
            return null;
        }
    }

    public static void writeState(Path path, State state) {
        if (path == null || state == null) return;
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            // We intentionally persist epoch millis for lastSeenTimestamp for stable numeric resume,
            // while readState accepts ISO strings for manual debugging.
            String json = GSON.toJson(state);
            Files.writeString(path, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[poller] Failed to write state: " + e.getMessage());
        }
    }
}
