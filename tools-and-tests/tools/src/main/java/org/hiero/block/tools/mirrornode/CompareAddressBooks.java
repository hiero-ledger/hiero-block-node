// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Compare two address book history JSON files and report discrepancies by day.
 */
@Command(
        name = "compareAddressBooks",
        description = "Compare two address book history JSON files and report discrepancies")
public class CompareAddressBooks implements Runnable {

    @Option(
            names = {"--old"},
            required = true,
            description = "Path to the old address book history JSON file")
    private Path oldFile;

    @Option(
            names = {"--new"},
            required = true,
            description = "Path to the new address book history JSON file")
    private Path newFile;

    @Option(
            names = {"--verbose", "-v"},
            description = "Show detailed differences for each discrepancy")
    private boolean verbose = false;

    @Option(
            names = {"--print-all-dates"},
            description = "Print all dates from both files for comparison")
    private boolean printAllDates = false;

    @Override
    public void run() {
        try {
            System.out.println("Loading address book histories...");
            AddressBookHistory oldHistory = loadAddressBookHistory(oldFile);
            AddressBookHistory newHistory = loadAddressBookHistory(newFile);

            System.out.println("Old file: " + oldHistory.addressBooks().size() + " entries");
            System.out.println("New file: " + newHistory.addressBooks().size() + " entries");

            Map<LocalDate, DatedNodeAddressBook> oldByDay = groupByDay(oldHistory.addressBooks());
            Map<LocalDate, DatedNodeAddressBook> newByDay = groupByDay(newHistory.addressBooks());

            if (printAllDates) {
                printAllDatesComparison(oldByDay, newByDay);
            } else {
                System.out.println("\nComparing address books...\n");
                compareAndReport(oldByDay, newByDay);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private AddressBookHistory loadAddressBookHistory(Path file) throws IOException {
        if (!Files.exists(file)) {
            throw new IOException("File not found: " + file);
        }

        try (var in = new ReadableStreamingData(Files.newInputStream(file))) {
            return AddressBookHistory.JSON.parse(in);
        } catch (Exception e) {
            throw new IOException("Failed to parse address book history from " + file + ": " + e.getMessage(), e);
        }
    }

    private Map<LocalDate, DatedNodeAddressBook> groupByDay(List<DatedNodeAddressBook> addressBooks) {
        return addressBooks.stream()
                .collect(Collectors.toMap(
                        dab -> toLocalDate(dab.blockTimestampOrThrow()),
                        dab -> dab,
                        (existing, replacement) -> replacement, // Keep last entry for the day
                        TreeMap::new));
    }

    private LocalDate toLocalDate(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.seconds(), timestamp.nanos())
                .atZone(ZoneOffset.UTC)
                .toLocalDate();
    }

    private void printAllDatesComparison(
            Map<LocalDate, DatedNodeAddressBook> oldByDay, Map<LocalDate, DatedNodeAddressBook> newByDay) {

        Set<LocalDate> allDays = new TreeSet<>();
        allDays.addAll(oldByDay.keySet());
        allDays.addAll(newByDay.keySet());

        System.out.println("\n" + "=".repeat(80));
        System.out.println("ALL DATES COMPARISON");
        System.out.println("=".repeat(80));
        System.out.printf("%-15s %-12s %-12s %-10s%n", "Date", "In OLD", "In NEW", "Status");
        System.out.println("-".repeat(80));

        for (LocalDate day : allDays) {
            boolean inOld = oldByDay.containsKey(day);
            boolean inNew = newByDay.containsKey(day);
            String status;

            if (inOld && inNew) {
                status = "✓ BOTH";
            } else if (inOld) {
                status = "❌ OLD ONLY";
            } else {
                status = "❌ NEW ONLY";
            }

            System.out.printf("%-15s %-12s %-12s %-10s%n", day, inOld ? "YES" : "NO", inNew ? "YES" : "NO", status);
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("SUMMARY");
        System.out.println("=".repeat(80));
        System.out.println("Total unique dates:         " + allDays.size());
        System.out.println("Dates in OLD file:          " + oldByDay.size());
        System.out.println("Dates in NEW file:          " + newByDay.size());

        long inBoth = allDays.stream()
                .filter(d -> oldByDay.containsKey(d) && newByDay.containsKey(d))
                .count();
        long oldOnly = allDays.stream()
                .filter(d -> oldByDay.containsKey(d) && !newByDay.containsKey(d))
                .count();
        long newOnly = allDays.stream()
                .filter(d -> !oldByDay.containsKey(d) && newByDay.containsKey(d))
                .count();

        System.out.println("Dates in BOTH files:        " + inBoth);
        System.out.println("Dates in OLD ONLY:          " + oldOnly);
        System.out.println("Dates in NEW ONLY:          " + newOnly);
    }

    private void compareAndReport(
            Map<LocalDate, DatedNodeAddressBook> oldByDay, Map<LocalDate, DatedNodeAddressBook> newByDay) {

        Set<LocalDate> allDays = new TreeSet<>();
        allDays.addAll(oldByDay.keySet());
        allDays.addAll(newByDay.keySet());

        int discrepancyCount = 0;
        int matchCount = 0;
        int missingInOldCount = 0;
        int missingInNewCount = 0;

        List<LocalDate> missingInOld = new ArrayList<>();
        List<LocalDate> missingInNew = new ArrayList<>();

        for (LocalDate day : allDays) {
            DatedNodeAddressBook oldEntry = oldByDay.get(day);
            DatedNodeAddressBook newEntry = newByDay.get(day);

            if (oldEntry == null) {
                missingInOldCount++;
                missingInOld.add(day);
                System.out.println("❌ " + day + " - MISSING in OLD file");
                if (verbose && newEntry != null) {
                    printAddressBookDetails("NEW", newEntry.addressBook());
                }
            } else if (newEntry == null) {
                missingInNewCount++;
                missingInNew.add(day);
                System.out.println("❌ " + day + " - MISSING in NEW file");
                if (verbose) {
                    printAddressBookDetails("OLD", oldEntry.addressBook());
                }
            } else {
                // Both exist - compare them
                AddressBookComparison comparison = compareAddressBooks(oldEntry.addressBook(), newEntry.addressBook());
                if (!comparison.isEqual()) {
                    discrepancyCount++;
                    System.out.println("❌ " + day + " - DISCREPANCY FOUND");
                    if (verbose) {
                        printComparison(comparison);
                    } else {
                        System.out.println("   " + comparison.getSummary());
                    }
                } else {
                    matchCount++;
                    if (verbose) {
                        System.out.println("✓ " + day + " - MATCH");
                    }
                }
            }
        }

        // Print summary
        System.out.println("\n" + "=".repeat(80));
        System.out.println("COMPARISON SUMMARY");
        System.out.println("=".repeat(80));
        System.out.println("Total days compared:        " + allDays.size());
        System.out.println("Matching days:              " + matchCount);
        System.out.println("Days with discrepancies:    " + discrepancyCount);
        System.out.println("Missing in OLD file:        " + missingInOldCount);
        System.out.println("Missing in NEW file:        " + missingInNewCount);

        // Print details of missing dates
        if (!missingInOld.isEmpty()) {
            System.out.println("\nDates MISSING in OLD file:");
            missingInOld.forEach(date -> System.out.println("  - " + date));
        }

        if (!missingInNew.isEmpty()) {
            System.out.println("\nDates MISSING in NEW file:");
            missingInNew.forEach(date -> System.out.println("  - " + date));
        }

        if (discrepancyCount > 0 || missingInOldCount > 0 || missingInNewCount > 0) {
            System.out.println("\n⚠️  Address books DO NOT MATCH");
            System.exit(1);
        } else {
            System.out.println("\n✓ Address books MATCH");
        }
    }

    private AddressBookComparison compareAddressBooks(NodeAddressBook oldBook, NodeAddressBook newBook) {
        List<NodeAddress> oldNodes = oldBook.nodeAddress();
        List<NodeAddress> newNodes = newBook.nodeAddress();

        if (oldNodes.size() != newNodes.size()) {
            return new AddressBookComparison(
                    false, "Node count mismatch: OLD=" + oldNodes.size() + ", NEW=" + newNodes.size(), null, null);
        }

        // Compare each node
        Map<Long, NodeAddress> oldNodesMap =
                oldNodes.stream().collect(Collectors.toMap(AddressBookRegistry::getNodeAccountId, n -> n));

        Map<Long, NodeAddress> newNodesMap =
                newNodes.stream().collect(Collectors.toMap(AddressBookRegistry::getNodeAccountId, n -> n));

        Set<Long> allNodeIds = new TreeSet<>();
        allNodeIds.addAll(oldNodesMap.keySet());
        allNodeIds.addAll(newNodesMap.keySet());

        Map<Long, String> differences = new HashMap<>();

        for (Long nodeId : allNodeIds) {
            NodeAddress oldNode = oldNodesMap.get(nodeId);
            NodeAddress newNode = newNodesMap.get(nodeId);

            if (oldNode == null) {
                differences.put(nodeId, "Node missing in OLD");
            } else if (newNode == null) {
                differences.put(nodeId, "Node missing in NEW");
            } else {
                String diff = compareNodes(oldNode, newNode);
                if (diff != null) {
                    differences.put(nodeId, diff);
                }
            }
        }

        if (differences.isEmpty()) {
            return new AddressBookComparison(true, "All nodes match", oldNodesMap, newNodesMap);
        } else {
            String summary = differences.size() + " node(s) have differences";
            return new AddressBookComparison(false, summary, oldNodesMap, newNodesMap, differences);
        }
    }

    private String compareNodes(NodeAddress oldNode, NodeAddress newNode) {
        List<String> diffs = new java.util.ArrayList<>();

        if (!oldNode.ipAddress().equals(newNode.ipAddress())) {
            diffs.add("IP: " + oldNode.ipAddress() + " -> " + newNode.ipAddress());
        }
        if (oldNode.portno() != newNode.portno()) {
            diffs.add("Port: " + oldNode.portno() + " -> " + newNode.portno());
        }
        if (!oldNode.rsaPubKey().equals(newNode.rsaPubKey())) {
            diffs.add("RSA key changed");
        }
        if (!oldNode.description().equals(newNode.description())) {
            diffs.add("Description: '" + oldNode.description() + "' -> '" + newNode.description() + "'");
        }

        return diffs.isEmpty() ? null : String.join(", ", diffs);
    }

    private void printAddressBookDetails(String label, NodeAddressBook book) {
        System.out.println(
                "   " + label + " address book: " + book.nodeAddress().size() + " nodes");
        book.nodeAddress()
                .forEach(node -> System.out.println("      Node " + node.nodeId() + ": " + node.ipAddress() + ":"
                        + node.portno() + " - " + node.description()));
    }

    private void printComparison(AddressBookComparison comparison) {
        System.out.println("   " + comparison.summary);
        if (comparison.differences != null) {
            comparison.differences.forEach((nodeId, diff) -> System.out.println("      Node " + nodeId + ": " + diff));
        }
    }

    private record AddressBookComparison(
            boolean isEqual,
            String summary,
            Map<Long, NodeAddress> oldNodes,
            Map<Long, NodeAddress> newNodes,
            Map<Long, String> differences) {

        AddressBookComparison(
                boolean isEqual, String summary, Map<Long, NodeAddress> oldNodes, Map<Long, NodeAddress> newNodes) {
            this(isEqual, summary, oldNodes, newNodes, null);
        }

        String getSummary() {
            return summary;
        }
    }
}
