// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.RecordFileDates;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Randomly samples (date, hour) combinations and compares:
 *  <ul>
 *    <li>number of signature files reported by the GCP mainnet bucket listing (via {@link MainNetBucket#listHour(long, boolean)})</li>
 *    <li>number of local signature files derived from compressed day {@code .tar.zst} archives</li>
 *  </ul>
 *
 * For each sampled (date, hour) it prints a per-timestamp table of signature counts:
 *
 * <pre>
 *   Timestamp                  gcpSig   localSig  diff
 *   2019-09-13T21:53:51Z         1        1        OK
 *   2019-09-13T21:54:12Z         2        1        MISMATCH
 * </pre>
 *
 * where each timestamp represents a record file start time. Only summary counts are printed; the
 * underlying file paths are not shown in the output.
 */
@Command(
        name = "sig-validation-ls",
        description = "Randomly sample (day, hour) and compare GCP LS vs local downloadedDays signature counts")
public class SignatureValidationListingCommand implements Runnable {

    private static final int MAX_SAMPLES = 20;
    private static final int MIN_NODE_ACCOUNT_ID = 3;
    private static final int MAX_NODE_ACCOUNT_ID = 37;

    @Option(
            names = "--samples",
            description = "Number of random (day, hour) samples (max " + MAX_SAMPLES + ")",
            defaultValue = "10")
    private int samples;

    @Option(names = "--start-date", required = true, description = "Start date (inclusive), yyyy-MM-dd")
    private LocalDate startDate;

    @Option(names = "--end-date", required = true, description = "End date (inclusive), yyyy-MM-dd")
    private LocalDate endDate;

    @Option(
            names = {"--compressed-day-or-days"},
            description = "One or more compressed day .tar.zst paths or directories to scan for local signatures")
    private File[] compressedDayOrDaysDirs = new File[0];

    @Option(
            names = {"-c", "--cache-dir"},
            description = "Directory for GCS cache (default: data/gcp-cache)",
            defaultValue = "data/gcp-cache")
    private File cacheDir;

    @Option(
            names = {"--cache"},
            description = "Enable GCS caching (default: false)",
            defaultValue = "false")
    private boolean cacheEnabled;

    @Option(
            names = {"-p", "--user-project"},
            description = "GCP project to bill for requester-pays bucket access (default: from GCP_PROJECT_ID env var)")
    private String userProject = DownloadConstants.GCP_PROJECT_ID;

    /**
     * GCP mainnet bucket helper.
     * Uses {@link MainNetBucket#listHour(long, boolean)} where the first argument is a block time
     * in nanoseconds since stream start (derived from a UTC {@link Instant}).
     */
    private MainNetBucket mainNetBucket;

    @Override
    public void run() {
        try {
            if (mainNetBucket == null) {
                mainNetBucket = new MainNetBucket(
                        cacheEnabled, cacheDir.toPath(), MIN_NODE_ACCOUNT_ID, MAX_NODE_ACCOUNT_ID, userProject);
            }

            if (compressedDayOrDaysDirs == null || compressedDayOrDaysDirs.length == 0) {
                throw new IllegalArgumentException("--compressed-day-or-days must be provided and not empty");
            }

            validateInputs();

            System.out.printf("Sampling %d (day, hour) slots between %s and %s%n", samples, startDate, endDate);

            final LocalDateTime startDateTime = startDate.atStartOfDay();
            final LocalDateTime endDateTimeExclusive = endDate.plusDays(1).atStartOfDay();
            final long totalHours = ChronoUnit.HOURS.between(startDateTime, endDateTimeExclusive);

            if (totalHours <= 0) {
                throw new IllegalArgumentException("Date range must cover at least one hour");
            }

            AtomicInteger sampleIndex = new AtomicInteger(1);

            new Random()
                    .longs(0, totalHours)
                    .distinct()
                    .limit(samples)
                    .mapToObj(startDateTime::plusHours)
                    .sorted()
                    .forEach(dt -> handleSample(sampleIndex.getAndIncrement(), dt.toLocalDate(), dt.getHour()));

        } catch (Exception e) {
            System.err.println("Error during sig-validation-ls: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void handleSample(final int sampleIndex, final LocalDate sampleDate, final int hour) {

        System.out.printf("%n=== Sample %d: date=%s hour=%02d ===%n", sampleIndex, sampleDate, hour);

        // Always compare GCP vs local signature counts for this hour
        HourResult result = compareHour(sampleDate, hour);

        System.out.printf("Matching record timestamps: %d%n", result.sameCount());
        if (result.diffs().isEmpty()) {
            System.out.println("All signature counts match for this hour.");
            return;
        }

        System.out.printf("%-27s %-8s %-9s %-5s%n", "Timestamp", "gcpSig", "localSig", "diff");
        for (PerTimestampDiff d : result.diffs()) {
            long diff = d.gcpCount() - d.localCount();
            String status = (diff == 0) ? "OK" : "MISMATCH";
            System.out.printf("%-27s %-8d %-9d %-5s%n", d.timestamp(), d.gcpCount(), d.localCount(), status);
        }
    }

    private void validateInputs() {
        if (endDate.isBefore(startDate)) {
            throw new IllegalArgumentException("end-date must be >= start-date");
        }
        if (samples <= 0) {
            throw new IllegalArgumentException("--samples must be > 0 (got " + samples + ")");
        }
        if (samples > MAX_SAMPLES) {
            throw new IllegalArgumentException(String.format(
                    "--samples must be <= %d to avoid overwhelming output (got %d)", MAX_SAMPLES, samples));
        }
    }

    private boolean isSignatureName(String name) {
        return name.endsWith(".rcd_sig");
    }

    /**
     * Extract just the file name from a GCS-style path like
     * {@code recordstreams/record0.0.3/2019-09-14T02_00_00.454287001Z.rcd_sig}.
     */
    private String extractFileName(String path) {
        int idx = path.lastIndexOf('/');
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    /**
     * Build a summary of record start time -> count of signature files in GCP for a given
     * (date, hour).
     *
     * The key is the record time as an {@link Instant}, derived from the record file name via
     * {@link RecordFileDates#extractRecordFileTime(String)}. Only signature objects
     * ({@code *.rcd_sig}) are considered.
     */
    protected Map<Instant, Long> buildGcpSigSummary(LocalDate date, int hour) {
        Map<Instant, Long> blockTimeToCount = new HashMap<>();

        try {
            // Convert (date, hour) into a block time in nanos since stream start (2019),
            // then ask MainNetBucket for that hour's listing.
            Instant instant = date.atTime(hour, 0).toInstant(ZoneOffset.UTC);
            long blockTimeNanos = RecordFileDates.instantToBlockTimeLong(instant);

            blockTimeToCount = mainNetBucket.listHour(blockTimeNanos, false).stream()
                    .map(cf -> cf.path())
                    .filter(this::isSignatureName)
                    .map(this::extractFileName)
                    .collect(Collectors.groupingBy(
                            RecordFileDates::extractRecordFileTime, // Extract block time
                            Collectors.counting() // Count occurrences
                            ));

        } catch (Exception e) {
            e.printStackTrace();
            System.err.printf("Error listing GCP signatures for date=%s hour=%02d: %s%n", date, hour, e.getMessage());
        }

        return blockTimeToCount;
    }

    /**
     * Build a summary of record start time -> count of signature files in the local compressed
     * day archives for a given (date, hour).
     *
     * The local counts are derived from {@link UnparsedRecordBlock} entries streamed from
     * {@code .tar.zst} files; only entries whose {@link UnparsedRecordBlock#recordFileTime()}
     * falls within the requested (date, hour) in UTC are included.
     */
    protected Map<Instant, Long> buildLocalSigSummary(LocalDate date, int hour) {
        Map<Instant, Long> blockTimeToCount = new HashMap<>();

        // Use compressed day .tar.zst files to derive local signature counts per record time.
        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(compressedDayOrDaysDirs);
        for (Path dayFile : dayPaths) {
            try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayFile)) {
                stream.filter((UnparsedRecordBlock set) -> set.recordFileTime() != null)
                        .forEach((UnparsedRecordBlock set) -> {
                            Instant recordTime = set.recordFileTime();

                            // Ensure we only count entries for the requested date + hour.
                            var zdt = recordTime.atZone(ZoneOffset.UTC);
                            if (!zdt.toLocalDate().equals(date) || zdt.getHour() != hour) {
                                return;
                            }

                            long signatureFileCount = set.signatureFiles().size();
                            blockTimeToCount.merge(recordTime, signatureFileCount, Long::sum);
                        });
            } catch (Exception e) {
                System.err.println("Error reading local compressed day file " + dayFile + ": " + e.getMessage());
            }
        }

        return blockTimeToCount;
    }

    /**
     * Compare GCP vs local signature counts for a given (date, hour), grouped by record start
     * time.
     *
     * Timestamps that appear in only one of the sources (GCP or local) are always treated as
     * mismatches. A timestamp is counted as "same" only when it is present in both maps and the
     * counts are exactly equal.
     *
     * @param date the UTC calendar date to inspect
     * @param hour the hour-of-day (0-23) within that date
     * @return an {@link HourResult} containing the number of matching timestamps and a list of
     *         per-timestamp differences where GCP and local counts diverge
     */
    protected HourResult compareHour(LocalDate date, int hour) {
        Map<Instant, Long> gcpCounts = buildGcpSigSummary(date, hour);
        Map<Instant, Long> localCounts = buildLocalSigSummary(date, hour);

        // Union of all timestamps present in either source
        Set<Instant> allKeys = new TreeSet<>();
        allKeys.addAll(gcpCounts.keySet());
        allKeys.addAll(localCounts.keySet());

        int same = 0;
        List<PerTimestampDiff> diffs = new ArrayList<>();

        for (Instant ts : allKeys) {
            boolean inGcp = gcpCounts.containsKey(ts);
            boolean inLocal = localCounts.containsKey(ts);

            long gcp = gcpCounts.getOrDefault(ts, 0L);
            long local = localCounts.getOrDefault(ts, 0L);

            // If a timestamp exists only on one side, it is always a mismatch.
            if (!inGcp || !inLocal) {
                diffs.add(new PerTimestampDiff(ts, gcp, local));
                continue;
            }

            // When present in both, only counts that are exactly equal are considered "same".
            if (gcp == local) {
                same++;
            } else {
                diffs.add(new PerTimestampDiff(ts, gcp, local));
            }
        }

        return new HourResult(date, hour, same, diffs);
    }

    record PerTimestampDiff(Instant timestamp, long gcpCount, long localCount) {}

    record HourResult(LocalDate date, int hour, int sameCount, List<PerTimestampDiff> diffs) {}
}
