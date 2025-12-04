// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.model.TarZstdDayUtils.parseDayFromFileName;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.TarZstdDayUtils;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.TarReader;
import org.hiero.block.tools.utils.ZstCmdInputStream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Validates that each block in compressed day files contains signatures from all expected nodes
 * as defined in the address book for that day.
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(
        name = "validate-sig-counts",
        description = "Validate that all blocks have signatures from all expected nodes",
        mixinStandardHelpOptions = true)
public class ValidateSignatureCounts implements Runnable {

    /** Pattern to extract node account ID from signature file name like "node_0.0.10.rcs_sig" */
    private static final Pattern SIG_FILE_PATTERN = Pattern.compile("node_(\\d+\\.\\d+\\.\\d+)\\.rcs_sig");

    /** Pattern to identify block directories (timestamp format) */
    private static final Pattern BLOCK_DIR_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}_\\d{2}_\\d{2}\\.\\d+Z");

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-d", "--downloaded-days-dir"},
            description = "Directory where downloaded days are stored")
    private File compressedDaysDir = new File("compressedDays");

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
        }

        // Print overall summary
        printOverallSummary(totalBlocksProcessed, totalBlocksWithMissingSignatures, totalMissingByNode);
    }

    /**
     * Result of processing a single day.
     */
    private record DayResult(long blocksProcessed, long blocksWithMissingSignatures, Map<String, Long> missingByNode) {}

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
        Map<String, Set<String>> signaturesByBlock = new LinkedHashMap<>();

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
                            .computeIfAbsent(blockDir, k -> new HashSet<>())
                            .add(nodeAccountId);
                }
            });

        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("@|red Error reading day file: " + e.getMessage() + "|@"));
            e.printStackTrace();
            return new DayResult(0, 0, missingByNode);
        }

        // Now validate each block
        int missingBlocksReported = 0;
        int maxMissingBlocksToReport = 10;

        for (Map.Entry<String, Set<String>> entry : signaturesByBlock.entrySet()) {
            String blockTimestamp = entry.getKey();
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

                // Report first few blocks with missing signatures
                if (missingBlocksReported < maxMissingBlocksToReport) {
                    String blockTimeFormatted = formatBlockTimestamp(blockTimestamp);
                    System.out.println(Ansi.AUTO.string(String.format(
                            "    @|red ✗|@ @|yellow %s|@ @|white missing|@ @|red %d|@ @|white signature(s):|@ @|red %s|@",
                            blockTimeFormatted, missingNodes.size(), formatNodeList(missingNodes))));
                    missingBlocksReported++;
                }
            }
        }

        if (missingBlocksReported >= maxMissingBlocksToReport
                && blocksWithMissingSignatures > maxMissingBlocksToReport) {
            System.out.println(Ansi.AUTO.string(String.format(
                    "    @|yellow ... and %d more blocks with missing signatures|@",
                    blocksWithMissingSignatures - maxMissingBlocksToReport)));
        }

        // Print day summary
        printDaySummary(blocksProcessed, blocksWithMissingSignatures, missingByNode);

        return new DayResult(blocksProcessed, blocksWithMissingSignatures, missingByNode);
    }

    /**
     * Extract the block directory name from a file path.
     */
    private String extractBlockDirectory(String pathStr) {
        // Path format: "2024-06-18T00_00_00.001886911Z/node_0.0.10.rcs_sig"
        // or "2024-06-18/2024-06-18T00_00_00.001886911Z/node_0.0.10.rcs_sig"
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
     * Format a list of node account IDs for display.
     */
    private String formatNodeList(Set<String> nodes) {
        if (nodes.size() <= 5) {
            return String.join(", ", nodes.stream().sorted().toList());
        } else {
            List<String> sortedNodes = nodes.stream().sorted().toList();
            return String.join(", ", sortedNodes.subList(0, 5)) + " ... +" + (nodes.size() - 5) + " more";
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
            long blocksProcessed, long blocksWithMissingSignatures, Map<String, Long> missingByNode) {
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
        System.out.println();
    }

    private void printOverallSummary(
            long totalBlocksProcessed, long totalBlocksWithMissingSignatures, Map<String, Long> totalMissingByNode) {
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
        System.out.println();
    }
}
