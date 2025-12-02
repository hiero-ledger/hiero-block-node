// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Randomly samples (date, hour) combinations and compares:
 *  - number of signature files reported by Signature Validation listing
 *  - number of local signature files in downloadedDays directory
 *
 * For each sample, it prints a per-timestamp table:
 *
 *   Timestamp                  gcpSig   localSig  diff
 *   2019-09-13T21_53_51.396440Z  1        1        0
 *   2019-09-13T21_54_12.123456Z  2        1        1 MISMATCH
 *
 */
@Command(
        name = "sig-validation-ls",
        description = "Randomly sample (day, hour) and compare GCP LS vs local downloadedDays signature counts")
public class SignatureValidationListingCommand implements Runnable {

    private static final int MAX_SAMPLES = 20;

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
            names = "--show-ls",
            description = "Show full list of GCP/local record files per node/hour",
            defaultValue = "false")
    private boolean showLs;

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
     * GCP mainnet bucket helper; assumed to have:
     *  - listHour(LocalDate date, boolean includeSidecars)
     */
    private MainNetBucket mainNetBucket;
    private static int minNodeAccountId = 3;
    private static int maxNodeAccountId = 37;

    @Override
    public void run() {
        try {
            if (mainNetBucket == null) {
                mainNetBucket = new MainNetBucket(
                        cacheEnabled, cacheDir.toPath(), minNodeAccountId, maxNodeAccountId, userProject);
            }
            if (compressedDayOrDaysDirs == null) {
                throw new IllegalArgumentException("--compressed-day-or-days must be provided");
            }

            validateInputs();

            System.out.printf("Sampling %d (day, hour) slots between %s and %s%n", samples, startDate, endDate);

            long daysRange = ChronoUnit.DAYS.between(startDate, endDate) + 1;

            for (int i = 0; i < samples; i++) {
                long dayOffset = ThreadLocalRandom.current().nextLong(daysRange);
                LocalDate sampleDate = startDate.plusDays(dayOffset);
                int hour = ThreadLocalRandom.current().nextInt(24);

                handleSample(i + 1, sampleDate, hour, null);
            }

        } catch (Exception e) {
            System.err.println("Error during sig-validation-ls: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void handleSample(
            final int sampleIndex, final LocalDate sampleDate, final int hour, final java.util.concurrent.ExecutorService executor)
            throws java.util.concurrent.ExecutionException, InterruptedException {

        System.out.printf("%n=== Sample %d: date=%s hour=%02d ===%n", sampleIndex, sampleDate, hour);

        // Always compare GCP vs local signature counts for this hour
        HourResult result = compareHour(sampleDate, hour);

        System.out.printf("Matching record timestamps: %d%n", result.sameCount);
        if (result.diffs.isEmpty()) {
            System.out.println("All signature counts match for this hour.");
            return;
        }

        System.out.printf("%-27s %-8s %-9s %-5s%n", "Timestamp", "gcpSig", "localSig", "diff");
        for (PerTimestampDiff d : result.diffs) {
            int diff = d.gcpCount - d.localCount;
            String status = (diff == 0) ? "OK" : "MISMATCH";
            System.out.printf(
                    "%-27s %-8d %-9d %-5s%n",
                    d.timestamp, d.gcpCount, d.localCount, status);
            if (!d.gcpFiles.isEmpty()) {
                System.out.println("  GCP sig files:");
                for (String file : d.gcpFiles) {
                    System.out.println("    " + file);
                }
            }
            if (!d.localFiles.isEmpty()) {
                System.out.println("  Local sig files:");
                for (String file : d.localFiles) {
                    System.out.println("    " + file);
                }
            }
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

    private boolean isRecordName(String name) {
        return name.endsWith(".rcd") || name.endsWith(".rcd.gz");
    }

    private boolean isSignatureName(String name) {
        return name.endsWith(".rcd_sig");
    }

    /**
     * Build a summary of record timestamp -> count of signature files in GCP
     * and the list of underlying GCP signature file paths for a given date and hour.
     *
     * The key is the record timestamp (e.g. "2019-09-13T21_53_51.396440Z"),
     * derived from the ".rcd_sig" file name.
     */
    protected SigSummary buildGcpSigSummary(LocalDate date, int hour) {
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        java.util.Map<String, java.util.List<String>> filesByTimestamp = new java.util.HashMap<>();
        final String hourToken = date + "T" + String.format("%02d", hour) + "_";

        try {
            // Scan the whole day and then filter to the hour, as requested in review.
            var chainFilesList = mainNetBucket.listHour(date.toEpochDay(), false);

            chainFilesList.stream()
                    .map(cf -> cf.path())
                    .filter(path -> path.contains(hourToken))
                    .filter(this::isSignatureName) // ".rcd_sig" only
                    .forEach(path -> {
                        String fileName = extractFileName(path);
                        // Strip ".rcd_sig" suffix to get the record timestamp key.
                        String tsKey = fileName.substring(0, fileName.length() - ".rcd_sig".length());
                        counts.merge(tsKey, 1, Integer::sum);
                        filesByTimestamp
                                .computeIfAbsent(tsKey, k -> new java.util.ArrayList<>())
                                .add(path);
                    });
        } catch (Exception e) {
            System.err.printf(
                    "Error listing GCP signatures for date=%s hour=%02d: %s%n", date, hour, e.getMessage());
        }

        return new SigSummary(counts, filesByTimestamp);
    }

    /**
     * Build a summary of record timestamp -> count of signature files in the local compressed days
     * and the list of underlying local signature file names for a given date and hour.
     */
    protected SigSummary buildLocalSigSummary(LocalDate date, int hour) {
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        java.util.Map<String, java.util.List<String>> filesByTimestamp = new java.util.HashMap<>();

        // Use compressed day .tar.zst files to derive local signature counts per record timestamp.
        // We mirror the pattern from the Ls command: stream UnparsedRecordBlock entries and filter by time prefix.
        final String timePrefix = date + "T" + String.format("%02d", hour);

        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(compressedDayOrDaysDirs);
        for (Path dayFile : dayPaths) {
            try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayFile)) {
                stream.filter((UnparsedRecordBlock set) ->
                                set.recordFileTime() != null
                                        && set.recordFileTime().toString().startsWith(timePrefix))
                        .forEach((UnparsedRecordBlock set) -> {
                            // recordFileTime() gives us the logical timestamp key
                            String tsKey = set.recordFileTime().toString();
                            int signatureFileCount = set.signatureFiles().size();
                            counts.merge(tsKey, signatureFileCount, Integer::sum);

                            // Track individual local signature files for this timestamp
                            for (var sig : set.signatureFiles()) {
                                filesByTimestamp
                                        .computeIfAbsent(tsKey, k -> new java.util.ArrayList<>())
                                        .add(sig.toString());
                            }
                        });
            } catch (Exception e) {
                System.err.println("Error reading local compressed day file " + dayFile + ": " + e.getMessage());
            }
        }

        return new SigSummary(counts, filesByTimestamp);
    }

    /**
     * Compare GCP vs local signature counts for a given hour, grouped by record timestamp.
     */
    protected HourResult compareHour(LocalDate date, int hour) {
        // NOTE: --skip-local-compare currently ignored; always comparing GCP vs local as per review feedback.
        SigSummary gcpSummary = buildGcpSigSummary(date, hour);
        java.util.Map<String, Integer> gcpCounts = gcpSummary.counts;
        java.util.Map<String, java.util.List<String>> gcpFilesByTimestamp = gcpSummary.filesByTimestamp;

        SigSummary localSummary = buildLocalSigSummary(date, hour);
        java.util.Map<String, Integer> localCounts = localSummary.counts;
        java.util.Map<String, java.util.List<String>> localFilesByTimestamp = localSummary.filesByTimestamp;

        java.util.Set<String> allKeys = new java.util.TreeSet<>();
        allKeys.addAll(gcpCounts.keySet());
        allKeys.addAll(localCounts.keySet());

        int same = 0;
        java.util.List<PerTimestampDiff> diffs = new java.util.ArrayList<>();

        for (String ts : allKeys) {
            java.util.List<String> gcpFilesForTs = gcpFilesByTimestamp.getOrDefault(ts, java.util.List.of());
            java.util.List<String> localFilesForTs = localFilesByTimestamp.getOrDefault(ts, java.util.List.of());
            int gcp = gcpCounts.getOrDefault(ts, 0);
            int local = localCounts.getOrDefault(ts, 0);
            if (gcp == local) {
                same++;
            } else {
                diffs.add(new PerTimestampDiff(ts, gcp, local, gcpFilesForTs, localFilesForTs));
            }
        }

        return new HourResult(date, hour, same, diffs);
    }

    private String extractFileName(String path) {
        int idx = path.lastIndexOf('/');
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    static final class SigSummary {
        final java.util.Map<String, Integer> counts;
        final java.util.Map<String, java.util.List<String>> filesByTimestamp;

        protected SigSummary(
            java.util.Map<String, Integer> counts,
            java.util.Map<String, java.util.List<String>> filesByTimestamp) {
            this.counts = counts;
            this.filesByTimestamp = filesByTimestamp;
        }
    }

    static final class PerTimestampDiff {
        final String timestamp;
        final int gcpCount;
        final int localCount;
        final java.util.List<String> gcpFiles;
        final java.util.List<String> localFiles;

        private PerTimestampDiff(
                String timestamp,
                int gcpCount,
                int localCount,
                java.util.List<String> gcpFiles,
                java.util.List<String> localFiles) {
            this.timestamp = timestamp;
            this.gcpCount = gcpCount;
            this.localCount = localCount;
            this.gcpFiles = gcpFiles;
            this.localFiles = localFiles;
        }
    }

    static final class HourResult {
        final LocalDate date;
        final int hour;
        final int sameCount;
        final java.util.List<PerTimestampDiff> diffs;

        private HourResult(LocalDate date, int hour, int sameCount, java.util.List<PerTimestampDiff> diffs) {
            this.date = date;
            this.hour = hour;
            this.sameCount = sameCount;
            this.diffs = diffs;
        }
    }
}
