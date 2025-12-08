// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.model.TarZstdDayUtils.parseDayFromFileName;
import static org.hiero.block.tools.records.RecordFileDates.convertInstantToStringWithPadding;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.TarReader;
import org.hiero.block.tools.utils.ZstCmdInputStream;
import org.hiero.block.tools.utils.gcp.MainNetBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Validates that each block in compressed day files contains signatures from all expected nodes
 * as defined in the address book for that day.
 */
@SuppressWarnings({"CallToPrintStackTrace", "unused"})
@Command(
        name = "validate-sig-counts",
        description = "Validate that all blocks have signatures from all expected nodes",
        mixinStandardHelpOptions = true)
public class ValidateSignatureCounts implements Runnable {

    /** Pattern to extract node account ID from signature file name like "node_0.0.10.rcd_sig" */
    private static final Pattern SIG_FILE_PATTERN = Pattern.compile("node_(\\d+\\.\\d+\\.\\d+)\\.rc[ds]_sig");

    /** Pattern to identify block directories (timestamp format) */
    private static final Pattern BLOCK_DIR_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}_\\d{2}_\\d{2}\\.\\d+Z");

    @Option(
            names = {"-d", "--downloaded-days-dir"},
            description = "Directory where downloaded days are stored")
    private File compressedDaysDir = new File("compressedDays");

    @SuppressWarnings("FieldCanBeLocal")
    @Option(
            names = {"-a", "--check-all"},
            description = "Check ALL blocks against GCP bucket (not just samples). Much slower but comprehensive.")
    private boolean checkAllBlocks = false;

    @Option(
            names = {"-p", "--user-project"},
            description = "GCP project to bill for requester-pays bucket access (default: from GCP_PROJECT_ID env var)")
    private String userProject = DownloadConstants.GCP_PROJECT_ID;

    @Parameters(index = "0", description = "From year")
    private int fromYear;

    @Parameters(index = "1", description = "From month")
    private int fromMonth;

    @Parameters(index = "2", description = "From day")
    private int fromDay;

    @Parameters(index = "3", description = "To year")
    private int toYear;

    @Parameters(index = "4", description = "To month")
    private int toMonth;

    @Parameters(index = "5", description = "To day")
    private int toDay;

    @Override
    public void run() {
        // Validate parameters
        LocalDate fromDate = LocalDate.of(fromYear, fromMonth, fromDay);
        LocalDate toDate = LocalDate.of(toYear, toMonth, toDay);

        if (fromDate.isAfter(toDate)) {
            System.out.println(Ansi.AUTO.string("@|red Error: From date must be before or equal to To date|@"));
            return;
        }

        // Set up paths
        if (!compressedDaysDir.exists() || !compressedDaysDir.isDirectory()) {
            System.out.println(Ansi.AUTO.string(
                    "@|red Error: compressedDays directory not found at: " + compressedDaysDir + "|@"));
            return;
        }

        // Load address book registry
        Path addressBookFile = compressedDaysDir.toPath().resolve("addressBookHistory.json");
        AddressBookRegistry addressBookRegistry;
        if (Files.exists(addressBookFile)) {
            addressBookRegistry = new AddressBookRegistry(addressBookFile);
            System.out.println(
                    Ansi.AUTO.string("@|green ✓ Loaded address book history from: " + addressBookFile + "|@"));
        } else {
            addressBookRegistry = new AddressBookRegistry();
            System.out.println(Ansi.AUTO.string("@|yellow ⚠ Using genesis address book (no history file found)|@"));
        }

        printHeader();

        // Get sorted day paths
        List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir});

        // Filter to only include days in the specified range
        List<Path> filteredDayPaths = dayPaths.stream()
                .filter(path -> {
                    LocalDate dayDate = parseDayFromFileName(path.getFileName().toString());
                    return !dayDate.isBefore(fromDate) && !dayDate.isAfter(toDate);
                })
                .toList();

        if (filteredDayPaths.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow No day files found in the specified date range|@"));
            return;
        }

        System.out.println(Ansi.AUTO.string(String.format(
                "  @|white Processing|@ @|cyan %d|@ @|white days from|@ @|cyan %s|@ @|white to|@ @|cyan %s|@",
                filteredDayPaths.size(), fromDate, toDate)));
        System.out.println();

        // Track overall statistics
        Map<String, Long> totalMissingByNode = new HashMap<>();
        long totalBlocksProcessed = 0;
        long totalBlocksWithMissingSignatures = 0;

        // Track days with incomplete archives (signatures exist in bucket but missing from tar.zstd)
        List<LocalDate> daysWithIncompleteArchives = new ArrayList<>();

        // Process each day
        for (Path dayPath : filteredDayPaths) {
            LocalDate dayDate = parseDayFromFileName(dayPath.getFileName().toString());

            DayResult dayResult = processDay(dayPath, dayDate, addressBookRegistry);

            // Accumulate totals
            totalBlocksProcessed += dayResult.blocksProcessed;
            totalBlocksWithMissingSignatures += dayResult.blocksWithMissingSignatures;
            for (Map.Entry<String, Long> entry : dayResult.missingByNode.entrySet()) {
                totalMissingByNode.merge(entry.getKey(), entry.getValue(), Long::sum);
            }

            // Track days with incomplete archives
            if (dayResult.hasIncompleteArchive) {
                daysWithIncompleteArchives.add(dayDate);
            }
        }

        // Print overall summary
        printOverallSummary(
                totalBlocksProcessed, totalBlocksWithMissingSignatures, totalMissingByNode, daysWithIncompleteArchives);
    }

    /**
     * Result of processing a single day.
     *
     * @param blocksProcessed total blocks processed
     * @param blocksWithMissingSignatures blocks missing at least one signature
     * @param missingByNode count of missing signatures per node
     * @param hasIncompleteArchive true if any sampled missing signatures exist in bucket but not in tar.zstd
     * @param incompleteBlockCount count of sampled blocks where signatures exist in bucket but missing from archive
     */
    private record DayResult(
            long blocksProcessed,
            long blocksWithMissingSignatures,
            Map<String, Long> missingByNode,
            boolean hasIncompleteArchive,
            int incompleteBlockCount) {}

    /**
     * Information about a block with missing signatures to be checked against the bucket.
     */
    private record MissingBlockInfo(Instant blockTimestamp, Set<String> missingNodes) {}

    /**
     * Process a single day file.
     */
    private DayResult processDay(Path dayPath, LocalDate dayDate, AddressBookRegistry addressBookRegistry) {
        printSectionHeader(dayDate.toString());

        // Get expected nodes for this day from the address book
        NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(
                dayDate.atStartOfDay().toInstant(java.time.ZoneOffset.UTC));

        Set<String> expectedNodeAccountIds = new HashSet<>();
        for (NodeAddress nodeAddress : addressBook.nodeAddress()) {
            long accountNum = AddressBookRegistry.getNodeAccountId(nodeAddress);
            expectedNodeAccountIds.add("0.0." + accountNum);
        }

        System.out.println(Ansi.AUTO.string(String.format(
                "  @|white Expected nodes:|@ @|cyan %d|@ @|white (from address book)|@",
                expectedNodeAccountIds.size())));

        // Track statistics for this day
        Map<String, Long> missingByNode = new HashMap<>();
        long blocksProcessed = 0;
        long blocksWithMissingSignatures = 0;

        // Map to collect files per block directory
        Map<Instant, Set<String>> signaturesByBlock = new LinkedHashMap<>();

        try (InputStream zstIn = new ZstCmdInputStream(dayPath);
                Stream<InMemoryFile> files = TarReader.readTarContents(zstIn)) {

            files.forEach(file -> {
                String pathStr = file.path().toString();
                String fileName = file.path().getFileName().toString();

                // Extract the block directory name (timestamp)
                String blockDir = extractBlockDirectory(pathStr);
                if (blockDir == null) {
                    return; // Skip files not in block directories
                }

                // Check if this is a signature file
                Matcher matcher = SIG_FILE_PATTERN.matcher(fileName);
                if (matcher.matches()) {
                    String nodeAccountId = matcher.group(1);
                    signaturesByBlock
                            .computeIfAbsent(Instant.parse(blockDir), k -> new HashSet<>())
                            .add(nodeAccountId);
                }
            });

        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("@|red Error reading day file: " + e.getMessage() + "|@"));
            e.printStackTrace();
            return new DayResult(0, 0, missingByNode, false, 0);
        }

        // Collect blocks with missing signatures for bucket checking (sampling)
        List<MissingBlockInfo> blocksToCheck = new ArrayList<>();
        int maxMissingBlocksToReport = 10;

        // Now validate each block
        for (Map.Entry<Instant, Set<String>> entry : signaturesByBlock.entrySet()) {
            final Instant blockTimestamp = entry.getKey();
            Set<String> foundSignatures = entry.getValue();

            blocksProcessed++;

            // Find missing signatures
            Set<String> missingNodes = new HashSet<>(expectedNodeAccountIds);
            missingNodes.removeAll(foundSignatures);

            if (!missingNodes.isEmpty()) {
                blocksWithMissingSignatures++;

                // Track missing counts per node
                for (String missingNode : missingNodes) {
                    missingByNode.merge(missingNode, 1L, Long::sum);
                }

                // Collect blocks for bucket checking (sample first few)
                if (blocksToCheck.size() < maxMissingBlocksToReport) {
                    blocksToCheck.add(new MissingBlockInfo(blockTimestamp, new HashSet<>(missingNodes)));
                }
            }
        }

        // Track if this day has incomplete archive (signatures exist in bucket but not in tar.zstd)
        boolean hasIncompleteArchive = false;
        int incompleteBlockCount = 0;

        // Check blocks against the GCP bucket
        if (!blocksToCheck.isEmpty()) {
            // Create MainNetBucket instance (no caching for this use case, with user project for requester-pays)
            MainNetBucket bucket = new MainNetBucket(false, Path.of("data/gcp-cache"), 3, 37, userProject);

            if (checkAllBlocks) {
                // Comprehensive mode: fetch all signatures from bucket for this day and compare
                System.out.println(Ansi.AUTO.string(
                        "  @|white Fetching ALL signature files from GCP bucket for comprehensive comparison...|@"));

                final String dayPrefix = dayDate.toString(); // Format: YYYY-MM-DD
                final Map<Instant, Set<String>> bucketSignatures = bucket.listSignatureFilesForDay(dayPrefix);

                System.out.println(Ansi.AUTO.string(String.format(
                        "  @|white Found|@ @|cyan %,d|@ @|white blocks with signatures in bucket|@",
                        bucketSignatures.size())));

                // Now compare ALL blocks with missing signatures against bucket
                int blocksReported = 0;
                for (Map.Entry<Instant, Set<String>> entry : signaturesByBlock.entrySet()) {
                    Instant blockTimestamp = entry.getKey();
                    Set<String> tarSignatures = entry.getValue();

                    // Find missing signatures in tar.zstd
                    Set<String> missingInTar = new HashSet<>(expectedNodeAccountIds);
                    missingInTar.removeAll(tarSignatures);

                    if (!missingInTar.isEmpty()) {
                        // Get signatures that exist in bucket for this block
                        Set<String> bucketSigs = bucketSignatures.getOrDefault(blockTimestamp, Set.of());

                        // Find signatures that exist in bucket but are missing from tar.zstd
                        List<String> existsInBucketMissingInTar = new ArrayList<>();

                        for (String nodeId : missingInTar) {
                            if (bucketSigs.contains(nodeId)) {
                                existsInBucketMissingInTar.add(nodeId);
                            }
                        }

                        // Only report blocks where tar.zstd is missing signatures that exist in bucket
                        if (!existsInBucketMissingInTar.isEmpty()) {
                            hasIncompleteArchive = true;
                            incompleteBlockCount++;

                            String blockTimeFormatted = formatBlockTimestamp(blockTimestamp);
                            System.out.println(Ansi.AUTO.string(String.format(
                                    "    @|red ✗|@ @|yellow %s|@ @|red [%d EXIST in bucket but missing in tar: %s]|@",
                                    blockTimeFormatted,
                                    existsInBucketMissingInTar.size(),
                                    formatNodeListShort(existsInBucketMissingInTar))));
                            blocksReported++;
                        }
                    }
                }

                if (blocksReported == 0 && blocksWithMissingSignatures > 0) {
                    System.out.println(Ansi.AUTO.string(
                            "    @|green ✓ All missing signatures are also missing from the bucket|@"));
                }

            } else {
                // Sampling mode: check only first few blocks
                System.out.println(Ansi.AUTO.string("  @|white Checking sampled blocks against GCP bucket...|@"));

                for (MissingBlockInfo blockInfo : blocksToCheck) {
                    String blockTimeFormatted = formatBlockTimestamp(blockInfo.blockTimestamp);

                    // Check each missing node's signature in the bucket
                    List<String> missingInBucket = new ArrayList<>();
                    List<String> existsInBucket = new ArrayList<>();

                    for (String nodeAccountId : blockInfo.missingNodes) {
                        boolean existsInGcp = bucket.signatureFileExists(nodeAccountId,
                            convertInstantToStringWithPadding(blockInfo.blockTimestamp));
                        if (existsInGcp) {
                            existsInBucket.add(nodeAccountId);
                        } else {
                            missingInBucket.add(nodeAccountId);
                        }
                    }

                    // Track if any signatures exist in bucket but not in archive
                    if (!existsInBucket.isEmpty()) {
                        hasIncompleteArchive = true;
                        incompleteBlockCount++;
                    }

                    // Format the output based on what we found
                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format(
                            "    @|red ✗|@ @|yellow %s|@ @|white missing|@ @|red %d|@ @|white sig(s):|@",
                            blockTimeFormatted, blockInfo.missingNodes.size()));

                    if (!missingInBucket.isEmpty() && existsInBucket.isEmpty()) {
                        // All missing signatures are also missing from bucket
                        sb.append(String.format(" @|green [all %d missing in bucket too]|@", missingInBucket.size()));
                    } else if (missingInBucket.isEmpty() && !existsInBucket.isEmpty()) {
                        // All missing signatures exist in bucket - tar.zstd is incomplete
                        sb.append(String.format(
                                " @|red [all %d EXIST in bucket - tar.zstd incomplete: %s]|@",
                                existsInBucket.size(), formatNodeListShort(existsInBucket)));
                    } else {
                        // Mixed: some missing in bucket, some exist
                        if (!missingInBucket.isEmpty()) {
                            sb.append(String.format(" @|green [%d missing in bucket]|@", missingInBucket.size()));
                        }
                        if (!existsInBucket.isEmpty()) {
                            sb.append(String.format(
                                    " @|red [%d EXIST in bucket - tar incomplete: %s]|@",
                                    existsInBucket.size(), formatNodeListShort(existsInBucket)));
                        }
                    }

                    System.out.println(Ansi.AUTO.string(sb.toString()));
                }

                if (blocksWithMissingSignatures > maxMissingBlocksToReport) {
                    System.out.println(Ansi.AUTO.string(String.format(
                            "    @|yellow ... and %d more blocks with missing signatures (not checked against bucket)|@",
                            blocksWithMissingSignatures - maxMissingBlocksToReport)));
                }
            }
        }

        // Print day summary
        printDaySummary(blocksProcessed, blocksWithMissingSignatures, missingByNode, hasIncompleteArchive);

        return new DayResult(
                blocksProcessed,
                blocksWithMissingSignatures,
                missingByNode,
                hasIncompleteArchive,
                incompleteBlockCount);
    }

    /**
     * Extract the block directory name from a file path.
     */
    private String extractBlockDirectory(String pathStr) {
        // Path format: "2024-06-18T00_00_00.001886911Z/node_0.0.10.rcd_sig"
        // or "2024-06-18/2024-06-18T00_00_00.001886911Z/node_0.0.10.rcd_sig"
        String[] parts = pathStr.split("/");
        for (String part : parts) {
            if (BLOCK_DIR_PATTERN.matcher(part).matches()) {
                return part;
            }
        }
        return null;
    }

    /**
     * Format a block timestamp for display (convert underscores to colons).
     */
    private String formatBlockTimestamp(String timestamp) {
        // Convert 2024-06-18T00_00_00.001886911Z to 2024-06-18T00:00:00.001886911Z
        int tIndex = timestamp.indexOf('T');
        if (tIndex >= 0) {
            String datePart = timestamp.substring(0, tIndex + 1);
            String timePart = timestamp.substring(tIndex + 1);
            return datePart + timePart.replace('_', ':');
        }
        return timestamp;
    }

    /**
     * Format a block timestamp for display (convert underscores to colons).
     */
    private String formatBlockTimestamp(Instant timestamp) {
        return timestamp.toString();
    }

    /**
     * Format a list of node account IDs for short display.
     */
    private String formatNodeListShort(List<String> nodes) {
        List<String> sortedNodes = nodes.stream().sorted().toList();
        if (sortedNodes.size() <= 3) {
            return String.join(", ", sortedNodes);
        } else {
            return String.join(", ", sortedNodes.subList(0, 3)) + " ...+" + (sortedNodes.size() - 3);
        }
    }

    private void printHeader() {
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   SIGNATURE COUNT VALIDATION|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
    }

    private void printSectionHeader(String title) {
        System.out.println(Ansi.AUTO.string("@|bold,blue ▶ " + title + "|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────|@"));
    }

    private void printDaySummary(
            long blocksProcessed,
            long blocksWithMissingSignatures,
            Map<String, Long> missingByNode,
            boolean hasIncompleteArchive) {
        System.out.println();
        System.out.println(Ansi.AUTO.string("  @|bold Day Summary:|@"));
        System.out.println(
                Ansi.AUTO.string(String.format("    @|white Blocks processed:|@ @|cyan %,d|@", blocksProcessed)));

        if (blocksWithMissingSignatures == 0) {
            System.out.println(Ansi.AUTO.string("    @|green ✓ All blocks have complete signatures|@"));
        } else {
            System.out.println(Ansi.AUTO.string(String.format(
                    "    @|red ✗ Blocks with missing signatures:|@ @|red %,d|@ @|white (%.2f%%)|@",
                    blocksWithMissingSignatures, (100.0 * blocksWithMissingSignatures / blocksProcessed))));

            // Show top missing nodes
            if (!missingByNode.isEmpty()) {
                System.out.println(Ansi.AUTO.string("    @|white Missing signature counts by node:|@"));
                missingByNode.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .limit(10)
                        .forEach(entry -> System.out.println(Ansi.AUTO.string(String.format(
                                "      @|yellow %s|@: @|red %,d|@ @|white missing|@",
                                entry.getKey(), entry.getValue()))));

                if (missingByNode.size() > 10) {
                    System.out.println(Ansi.AUTO.string(
                            String.format("      @|yellow ... and %d more nodes|@", missingByNode.size() - 10)));
                }
            }
        }

        // Indicate if this day has incomplete archive
        if (hasIncompleteArchive) {
            System.out.println(
                    Ansi.AUTO.string(
                            "    @|red,bold ⚠ INCOMPLETE ARCHIVE: Some signatures exist in bucket but missing from tar.zstd|@"));
        }
        System.out.println();
    }

    private void printOverallSummary(
            long totalBlocksProcessed,
            long totalBlocksWithMissingSignatures,
            Map<String, Long> totalMissingByNode,
            List<LocalDate> daysWithIncompleteArchives) {
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   OVERALL SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();

        System.out.println(Ansi.AUTO.string(
                String.format("  @|white Total blocks processed:|@ @|cyan %,d|@", totalBlocksProcessed)));

        if (totalBlocksWithMissingSignatures == 0) {
            System.out.println(Ansi.AUTO.string("  @|green ✓ All blocks across all days have complete signatures|@"));
        } else {
            System.out.println(Ansi.AUTO.string(String.format(
                    "  @|red ✗ Total blocks with missing signatures:|@ @|red %,d|@ @|white (%.2f%%)|@",
                    totalBlocksWithMissingSignatures,
                    (100.0 * totalBlocksWithMissingSignatures / totalBlocksProcessed))));

            // Show all missing nodes sorted by count
            if (!totalMissingByNode.isEmpty()) {
                System.out.println();
                System.out.println(
                        Ansi.AUTO.string("  @|bold Missing signature counts by node (total across all days):|@"));
                totalMissingByNode.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .forEach(entry -> System.out.println(Ansi.AUTO.string(String.format(
                                "    @|yellow %-12s|@: @|red %,10d|@ @|white missing|@",
                                entry.getKey(), entry.getValue()))));
            }
        }

        // Show days with incomplete archives that need investigation
        if (!daysWithIncompleteArchives.isEmpty()) {
            System.out.println();
            System.out.println(
                    Ansi.AUTO.string("@|bold,red ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string(String.format(
                    "@|bold,red   ⚠ DAYS WITH INCOMPLETE ARCHIVES (%d days need investigation)|@",
                    daysWithIncompleteArchives.size())));
            System.out.println(
                    Ansi.AUTO.string("@|bold,red ════════════════════════════════════════════════════════════|@"));
            System.out.println();
            System.out.println(
                    Ansi.AUTO.string("  @|white These days have signatures that exist in the GCP bucket but are|@"));
            System.out.println(
                    Ansi.AUTO.string("  @|white missing from the tar.zstd archive (based on sampled blocks):|@"));
            System.out.println();

            for (LocalDate day : daysWithIncompleteArchives) {
                System.out.println(Ansi.AUTO.string(String.format("    @|red ✗|@ @|yellow %s|@", day)));
            }
            System.out.println();
        } else if (totalBlocksWithMissingSignatures > 0) {
            System.out.println();
            System.out.println(Ansi.AUTO.string(
                    "  @|green ✓ All sampled missing signatures are also missing from the GCP bucket|@"));
            System.out.println(
                    Ansi.AUTO.string("  @|green   (tar.zstd archives appear to be complete copies of bucket data)|@"));
        }

        System.out.println();
    }
}
