// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import io.helidon.http.NotFoundException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Randomly samples (date, hour) combinations and compares:
 *  - number of record files reported by GCP listing
 *  - number of local record files in downloadedDays directory
 *
 * For each sample, it prints a per-node table:
 *
 *   Node     gcpFiles  localFiles  diff      status
 *   0.0.0    42        42          0         OK
 *   0.0.1    41        40          1         MISMATCH
 *
 */
@Command(
        name = "gcp-ls",
        description = "Randomly sample (day, hour) and compare GCP LS vs local downloadedDays signature counts")
public class GcpLsCommand implements Runnable {

    @Option(names = "--start-date", required = true, description = "Start date (inclusive), yyyy-MM-dd")
    private LocalDate startDate;

    @Option(names = "--end-date", required = true, description = "End date (inclusive), yyyy-MM-dd")
    private LocalDate endDate;

    @Option(names = "--samples", description = "Number of random (day, hour) samples", defaultValue = "10")
    private int samples;

    @Option(
            names = "--downloaded-days-dir",
            description = "Root of downloadedDays directory (YYYY/MM/DD/record0.0.<node> structure)")
    private Path downloadedDaysDir;

    @Option(
            names = "--node-count",
            description = "Number of record nodes (record0.0.x), usually 31 for 0..30",
            defaultValue = "31")
    private int nodeCount;

    @Option(
            names = "--skip-local-compare",
            description = "Skip comparing against local downloadedDays dir; only show GCP signature counts")
    private boolean skipLocalCompare = false;

    @Option(names = "--show-ls", description = "Show full list of GCP/local record files per node/hour")
    private boolean showLs = false;

    @Option(
            names = {"-c", "--cache-dir"},
            description = "Directory for GCS cache (default: data/gcp-cache)")
    private File cacheDir = new File("data/gcp-cache");

    @Option(
            names = {"--cache"},
            description = "Enable GCS caching (default: false)")
    private boolean cacheEnabled = false;

    @Option(
            names = {"--min-node"},
            description = "Minimum node account ID (default: 3)")
    private int minNodeAccountId = 3;

    @Option(
            names = {"--max-node"},
            description = "Maximum node account ID (default: 37)")
    private int maxNodeAccountId = 37;

    @Option(names = "--parallelism", description = "Thread pool size for parallel node queries", defaultValue = "8")
    private int parallelism;

    @Option(
            names = {"-p", "--user-project"},
            description = "GCP project to bill for requester-pays bucket access (default: from GCP_PROJECT_ID env var)")
    private String userProject = DownloadConstants.GCP_PROJECT_ID;

    /**
     * GCP mainnet bucket helper; assumed to have:
     *  - listHour(LocalDate date, int hour, boolean includeSidecars)
     */
    private final MainNetBucket mainNetBucket;

    /**
     * Typical wiring: new CommandLine(new GcpLsCommand(mainNetBucket)).execute(args);
     */
    public GcpLsCommand() {
        this.mainNetBucket =
                new MainNetBucket(true, cacheDir.toPath(), minNodeAccountId, maxNodeAccountId, userProject);
    }

    @Override
    public void run() {
        try {
            if (!skipLocalCompare && downloadedDaysDir == null) {
                throw new NotFoundException("DownloadedDaysDir has not been set");
            }

            validateInputs();

            System.out.printf(
                    "Sampling %d (day, hour) slots between %s and %s%n", samples, startDate, endDate);

            long daysRange = ChronoUnit.DAYS.between(startDate, endDate) + 1;

            ExecutorService executor = Executors.newFixedThreadPool(parallelism);
            try {
                for (int i = 0; i < samples; i++) {
                    long dayOffset = ThreadLocalRandom.current().nextLong(daysRange);
                    LocalDate sampleDate = startDate.plusDays(dayOffset);
                    int hour = ThreadLocalRandom.current().nextInt(24);

                    handleSample(i + 1, sampleDate, hour, executor);
                }
            } finally {
                executor.shutdownNow();
            }

        } catch (Exception e) {
            System.err.println("Error during gcp-ls: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void handleSample(
            final int sampleIndex, final LocalDate sampleDate, final int hour, final ExecutorService executor)
            throws ExecutionException, InterruptedException {

        System.out.printf("%n=== Sample %d: date=%s hour=%02d ===%n", sampleIndex, sampleDate, hour);
        System.out.printf("%-8s %-10s %-10s %-10s %-8s%n", "Node", "gcpFiles", "localFiles", "diff", "status");

        // submit node tasks in parallel
        final List<Future<NodeResult>> futures = new ArrayList<>();
        for (int node = 0; node < nodeCount; node++) {
            final int nodeId = node;
            futures.add(executor.submit(() -> checkNodeForHour(sampleDate, hour, nodeId)));
        }

        // gather results in node order
        final List<NodeResult> results = new ArrayList<>(futures.size());
        for (final Future<NodeResult> future : futures) {
            results.add(future.get());
        }

        // print summary table
        for (final NodeResult r : results) {
            String status = (r.diff == 0 && r.gcpSigCount >= 0) ? "OK" : "MISMATCH";
            if (r.gcpSigCount < 0) {
                status = "ERROR";
            }
            System.out.printf(
                    "%-8s %-10d %-10d %-10d %-8s%n",
                    "0.0." + r.node, r.gcpSigCount, r.localSigCount, r.diff, status);
        }

        printDifferenceView(results);
        printLsView(results);
    }

    private void printDifferenceView(List<NodeResult> results) {
        // enriched differences view
        if (!skipLocalCompare) {
            for (final NodeResult r : results) {
                if ((r.gcpOnly != null && !r.gcpOnly.isEmpty())
                        || (r.localOnly != null && !r.localOnly.isEmpty())) {
                    System.out.printf("    Differences for node 0.0.%d:%n", r.node);
                    if (r.gcpOnly != null && !r.gcpOnly.isEmpty()) {
                        System.out.println("      Present in GCP only:");
                        r.gcpOnly.forEach(f -> System.out.println("        " + f));
                    }
                    if (r.localOnly != null && !r.localOnly.isEmpty()) {
                        System.out.println("      Present in local only:");
                        r.localOnly.forEach(f -> System.out.println("        " + f));
                    }
                }
            }
        }
    }

    private void printLsView(List<NodeResult> results) {
        // optional full ls-style view
        if (showLs) {
            for (final NodeResult r : results) {
                System.out.printf("    LS for node 0.0.%d:%n", r.node);

                if (r.gcpFiles != null && !r.gcpFiles.isEmpty()) {
                    System.out.println("      GCP files:");
                    r.gcpFiles.forEach(f -> System.out.println("        " + f));
                } else {
                    System.out.println("      GCP files: (none)");
                }

                if (!skipLocalCompare) {
                    if (r.localFiles != null && !r.localFiles.isEmpty()) {
                        System.out.println("      Local files:");
                        r.localFiles.forEach(f -> System.out.println("        " + f));
                    } else {
                        System.out.println("      Local files: (none)");
                    }
                }
            }
        }
    }

    private void validateInputs() {
        if (endDate.isBefore(startDate)) {
            throw new IllegalArgumentException("end-date must be >= start-date");
        }
    }

    /**
     * Check one node for a given date+hour:
     *
     *  - GCP: uses MainNetBucket.listHour(date),
     *         then filters blobs for this node and record file extensions.
     *  - Local: counts *.rcd / *.rcd.gz files in downloadedDaysDir for date+hour+node.
     */
    private NodeResult checkNodeForHour(LocalDate date, int hour, int node) {
        List<String> gcpSigFiles = List.of();
        long gcpSigCount;
        try {
            var blobs = mainNetBucket.listHour(date.toEpochDay());

            final String hourToken = date + "T" + String.format("%02d", hour) + "_";

            gcpSigFiles = blobs.stream()
                    .map(b -> b.path())
                    // restrict to this hour based on filename in the path
                    .filter(path -> path.contains(hourToken))
                    // only record files
                    .filter(this::isRecordName)
                    // keep only this node's prefix, e.g. ".../record0.0.3/..."
                    .filter(path -> matchesNode(path, node))
                    .map(this::extractFileName)
                    .toList();

            gcpSigCount = gcpSigFiles.size();
        } catch (Exception e) {
            System.err.printf(
                    "Error listing GCP for node 0.0.%d date=%s hour=%02d: %s%n", node, date, hour, e.getMessage());
            gcpSigCount = -1; // sentinel meaning "GCP listing failed"
        }

        long localSigCount;
        long diff;
        List<String> gcpOnly = List.of();
        List<String> localOnly = List.of();

        if (skipLocalCompare) {
            localSigCount = -1;
            diff = 0;
        } else {
            List<String> localRecordFiles = listLocalRecordFiles(date, hour, node);
            localSigCount = localRecordFiles.size();
            diff = (gcpSigCount < 0) ? 0 : (gcpSigCount - localSigCount);

            if (gcpSigCount >= 0) {
                Set<String> gcpOnlySet = new TreeSet<>(gcpSigFiles);
                gcpOnlySet.removeAll(localRecordFiles);

                Set<String> localOnlySet = new TreeSet<>(localRecordFiles);
                localOnlySet.removeAll(gcpSigFiles);

                if (!gcpOnlySet.isEmpty()) {
                    gcpOnly = new ArrayList<>(gcpOnlySet);
                }
                if (!localOnlySet.isEmpty()) {
                    localOnly = new ArrayList<>(localOnlySet);
                }
            }
        }

        return new NodeResult(
                node,
                gcpSigCount,
                localSigCount,
                diff,
                gcpOnly,
                localOnly,
                gcpSigFiles,
                skipLocalCompare ? List.of() : listLocalRecordFiles(date, hour, node));
    }

    private boolean isRecordName(String name) {
        return name.endsWith(".rcd") || name.endsWith(".rcd.gz");
    }

    /**
     * If object names encode the node in the path (e.g. "recordstreams/record0.0.3/..."),
     * this checks that this blob belongs to the given node.
     *
     * If MainNetBucket.listHour(...) is already node-scoped, you can just "return true;" here.
     */
    private boolean matchesNode(String objectName, int node) {
        String needle = "record0.0." + node + "/";
        return objectName.contains(needle);
    }

    private String extractFileName(String path) {
        int idx = path.lastIndexOf('/');
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    private List<String> listLocalRecordFiles(LocalDate date, int hour, int node) {
        Path dayDir = downloadedDaysDir
                .resolve(String.valueOf(date.getYear()))
                .resolve(String.format("%02d", date.getMonthValue()))
                .resolve(String.format("%02d", date.getDayOfMonth()));

        // per-node subdirectory
        dayDir = dayDir.resolve("record0.0." + node);

        if (!Files.exists(dayDir)) {
            return List.of();
        }

        String filenamePrefix = date + "T" + String.format("%02d", hour) + "_";

        try (Stream<Path> stream = Files.list(dayDir)) {
            return stream.filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.startsWith(filenamePrefix))
                    .filter(this::isRecordName)
                    .toList();
        } catch (Exception e) {
            System.err.println("Error listing local record files in " + dayDir + ": " + e.getMessage());
            return List.of();
        }
    }

    private static final class NodeResult {
        final int node;
        final long gcpSigCount;
        final long localSigCount;
        final long diff;
        final List<String> gcpOnly;
        final List<String> localOnly;
        final List<String> gcpFiles;
        final List<String> localFiles;

        private NodeResult(
                int node,
                long gcpSigCount,
                long localSigCount,
                long diff,
                List<String> gcpOnly,
                List<String> localOnly,
                List<String> gcpFiles,
                List<String> localFiles) {
            this.node = node;
            this.gcpSigCount = gcpSigCount;
            this.localSigCount = localSigCount;
            this.diff = diff;
            this.gcpOnly = gcpOnly;
            this.localOnly = localOnly;
            this.gcpFiles = gcpFiles;
            this.localFiles = localFiles;
        }
    }
}
