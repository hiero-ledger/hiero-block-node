// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.hiero.block.tools.days.downloadlive.DateUtil.parseMirrorTimestamp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.block.tools.mirrornode.BlockInfo;
import org.hiero.block.tools.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.mirrornode.MirrorNodeBlockQueryOrder;

/**
 * Day-scoped live poller that queries the mirror node for latest blocks,
 * filters to the current day + unseen blocks, then delegates to the
 * LiveDownloader to fetch and place files.
 */
public class LivePoller {

    private static final Pattern P_DAY = Pattern.compile("\"dayKey\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern P_LAST = Pattern.compile("\"lastSeenBlock\"\\s*:\\s*(\\d+)");

    private final Duration interval;
    private final ZoneId tz;
    private final int batchSize;
    private long lastSeenBlock = -1L;
    private Instant lastSeenTimestamp = null;
    private final Path statePath;
    private final LiveDownloader downloader;
    private boolean stateLoadedForToday = false;
    // Optional global date range for ingestion.
    private final LocalDate configuredStartDay;
    private final LocalDate configuredEndDay;

    private static final Gson GSON = new GsonBuilder().create();

    public LivePoller(
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

    int runOnceForDay(LocalDate day) {
        final ZonedDateTime dayStart = day.atStartOfDay(ZoneId.of("UTC"));
        final ZonedDateTime dayEnd = dayStart.plusDays(1);
        final String dayKey = day.toString(); // YYYY-MM-DD

        if (!stateLoadedForToday) {
            State st = readState(statePath);
            if (st != null && dayKey.equals(st.getDayKey())) {
                lastSeenBlock = st.getLastSeenBlock();
                System.out.println(
                        "[poller] Resumed lastSeenBlock from state for day " + dayKey + ": " + lastSeenBlock);
            } else {
                lastSeenBlock = -1L;
                System.out.println(
                        "[poller] No matching state for " + dayKey + " (starting fresh from beginning of day).");
            }
            stateLoadedForToday = true;
        }
        System.out.println("[poller] dayKey=" + dayKey + " interval=" + interval + " batchSize=" + batchSize
                + " lastSeen=" + lastSeenBlock);

        final Instant windowStartInstant =
                (lastSeenTimestamp != null) ? lastSeenTimestamp.plusNanos(1) : dayStart.toInstant();
        final Instant windowEndInstant = dayEnd.toInstant();

        final long startSeconds = windowStartInstant.getEpochSecond();
        final long endSeconds = windowEndInstant.getEpochSecond();

        final List<String> timestampFilters = new ArrayList<>();
        timestampFilters.add("gte:" + startSeconds + ".000000000");
        timestampFilters.add("lt:" + endSeconds + ".000000000");

        final List<BlockInfo> latest =
                FetchBlockQuery.getLatestBlocks(batchSize, MirrorNodeBlockQueryOrder.ASC, timestampFilters);
        System.out.println("[poller] Latest blocks size: " + latest.size());

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
                    if (zts.isBefore(dayStart) || !zts.isBefore(dayEnd)) {
                        return null;
                    }
                    return new BlockDescriptor(b.number, b.name, ts.toString(), b.hash);
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(d -> d.getBlockNumber()))
                .toList();
        System.out.println("[poller] descriptors=" + batch.size());
        if (!batch.isEmpty()) {
            final long highestDownloaded = downloader.downloadBatch(dayKey, batch);
            if (highestDownloaded > lastSeenBlock) {
                lastSeenBlock = highestDownloaded;
            }
            // Track the timestamp of the highest block we just processed so that the next poll
            // window starts after this point, allowing us to walk the entire day in batches.
            BlockDescriptor lastDescriptor = batch.get(batch.size() - 1);
            lastSeenTimestamp = Instant.parse(lastDescriptor.getTimestampIso());
            batch.stream()
                    .limit(3)
                    .forEach(d -> System.out.println("[poller] sample -> block=" + d.getBlockNumber() + " file="
                            + d.getFilename() + " ts=" + d.getTimestampIso()));
            // Persist the current state so subsequent runs can resume from the latest known cursor.
            writeState(statePath, new State(dayKey, lastSeenBlock));
            return batch.size();
        } else {
            System.out.println("[poller] No new blocks this tick for " + dayKey + ".");
            return 0;
        }
    }

    public void runContinuouslyForToday() {
        LocalDate todayInTz =
                ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).toLocalDate();
        LocalDate logicalDay =
                (configuredStartDay != null && !configuredStartDay.isAfter(todayInTz)) ? configuredStartDay : todayInTz;

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
                // For historical days, reuse any persisted state (if present) so we can resume
                // long-running downloads. We always reset the in-memory timestamp cursor and
                // let runOnceForDay() rehydrate lastSeenBlock from state on the first tick.
                lastSeenTimestamp = null;
                stateLoadedForToday = false;
                while (true) {
                    int descriptors = runOnceForDay(logicalDay);
                    if (descriptors == 0) {
                        break;
                    }
                }
                try {
                    System.out.println("[poller] Finalizing historical day " + dayKey);
                    downloader.finalizeDay(dayKey);
                } catch (Exception e) {
                    System.err.println("[poller] Failed to finalize day archive for " + dayKey + ": " + e.getMessage());
                }
                logicalDay = logicalDay.plusDays(1);
                continue;
            }

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

    public static State readState(Path path) {
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

    public static void writeState(Path path, State state) {
        if (path == null || state == null) return;
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            String json = GSON.toJson(state);
            Files.writeString(path, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[poller] Failed to write state: " + e.getMessage());
        }
    }
}
