// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import picocli.CommandLine.Help.Ansi;

/**
 * Validates computed account balances against pre-fetched balance checkpoints.
 *
 * <p>This validator uses balance checkpoints that were pre-downloaded and verified
 * by {@link FetchBalanceCheckpointsCommand}. Unlike {@link BalanceCsvValidator},
 * this validator does not require GCP access at runtime since all checkpoints
 * are loaded from a local resource file.
 *
 * <p>The validator supports loading checkpoints from:
 * <ul>
 *   <li>A compiled resource file ({@code balance_checkpoints.gz})</li>
 *   <li>A directory of custom balance files ({@code accountBalances_{blockNumber}.pb.gz})</li>
 * </ul>
 */
public class BalanceCheckpointValidator {

    /** Approximate blocks per day on mainnet */
    private static final long BLOCKS_PER_DAY = 20_000L;

    /** The loader containing all balance checkpoints */
    private final BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();

    /** Block numbers of checkpoints that have been validated */
    private final List<Long> validatedCheckpoints = new ArrayList<>();

    /** Index into sorted checkpoint list for next checkpoint to validate */
    private int nextCheckpointIndex = 0;

    /** Sorted list of checkpoint block numbers */
    private List<Long> sortedCheckpointBlocks;

    /** Validation results */
    private final List<CheckpointResult> results = new ArrayList<>();

    /** Minimum block interval between checkpoint validations */
    private long minBlockInterval = 0;

    /**
     * Load balance checkpoints from the compiled resource file.
     *
     * @param checkpointsFile path to the balance_checkpoints.gz file
     * @throws IOException if the file cannot be read
     */
    public void loadFromFile(Path checkpointsFile) throws IOException {
        loader.loadFromCompiledFile(checkpointsFile);
        initializeSortedBlocks();
    }

    /**
     * Load balance checkpoints from a classpath resource stream.
     *
     * @param inputStream input stream of the balance_checkpoints.gz file
     * @throws IOException if the stream cannot be read
     */
    public void loadFromStream(InputStream inputStream) throws IOException {
        loader.loadFromStream(inputStream);
        initializeSortedBlocks();
    }

    /**
     * Load custom balance files from a directory.
     * Files must be named {@code accountBalances_{blockNumber}.pb.gz}.
     *
     * @param directory the directory containing custom balance files
     * @throws IOException if files cannot be read
     */
    public void loadFromDirectory(Path directory) throws IOException {
        loader.loadFromDirectory(directory);
        initializeSortedBlocks();
    }

    /**
     * Set the minimum interval between checkpoint validations.
     *
     * @param intervalDays minimum days between validations (0 = check all)
     */
    public void setCheckIntervalDays(int intervalDays) {
        this.minBlockInterval = intervalDays * BLOCKS_PER_DAY;
    }

    /**
     * Initialize the sorted list of checkpoint block numbers.
     */
    private void initializeSortedBlocks() {
        List<Long> allCheckpoints = new ArrayList<>(loader.getAllCheckpoints().keySet());

        // Filter checkpoints based on interval
        if (minBlockInterval > 0 && allCheckpoints.size() > 1) {
            sortedCheckpointBlocks = new ArrayList<>();
            sortedCheckpointBlocks.add(allCheckpoints.get(0)); // Always include first
            long lastIncluded = allCheckpoints.get(0);

            for (int i = 1; i < allCheckpoints.size(); i++) {
                long block = allCheckpoints.get(i);
                if (block - lastIncluded >= minBlockInterval) {
                    sortedCheckpointBlocks.add(block);
                    lastIncluded = block;
                }
            }
        } else {
            sortedCheckpointBlocks = allCheckpoints;
        }

        System.out.println(Ansi.AUTO.string("@|yellow Balance checkpoints loaded:|@ "
                + loader.getAllCheckpoints().size() + " total, " + sortedCheckpointBlocks.size()
                + " will be validated"));
        if (!sortedCheckpointBlocks.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow Block range:|@ " + sortedCheckpointBlocks.get(0) + " - "
                    + sortedCheckpointBlocks.get(sortedCheckpointBlocks.size() - 1)));
        }
    }

    /**
     * Check if the current block has passed any balance checkpoints that need validation.
     *
     * @param blockNumber the current block number
     * @param computedBalances the current computed balance map
     * @throws ValidationException if balance validation fails
     */
    public void checkBlock(long blockNumber, Map<Long, Long> computedBalances) throws ValidationException {
        if (sortedCheckpointBlocks == null || sortedCheckpointBlocks.isEmpty()) {
            return;
        }

        // Check if we've passed any checkpoints
        while (nextCheckpointIndex < sortedCheckpointBlocks.size()) {
            long checkpointBlock = sortedCheckpointBlocks.get(nextCheckpointIndex);
            if (blockNumber >= checkpointBlock) {
                validateCheckpoint(checkpointBlock, computedBalances);
                validatedCheckpoints.add(checkpointBlock);
                nextCheckpointIndex++;
            } else {
                break;
            }
        }
    }

    /**
     * Validate computed balances against a checkpoint.
     *
     * @param checkpointBlock the checkpoint block number
     * @param computedBalances the computed balance map
     * @throws ValidationException if validation fails
     */
    private void validateCheckpoint(long checkpointBlock, Map<Long, Long> computedBalances) throws ValidationException {
        System.out.println(Ansi.AUTO.string("\n@|cyan Validating balance checkpoint at block:|@ " + checkpointBlock));

        Map<Long, Long> expectedBalances = loader.getBalances(checkpointBlock);
        if (expectedBalances == null) {
            System.out.println(Ansi.AUTO.string("@|yellow Warning:|@ No checkpoint data for block " + checkpointBlock));
            results.add(new CheckpointResult(checkpointBlock, false, "No checkpoint data", 0, 0, 0));
            return;
        }

        // Compare balances
        ComparisonResult comparison = compareBalances(expectedBalances, computedBalances);

        // Record result
        results.add(new CheckpointResult(
                checkpointBlock,
                comparison.mismatches.isEmpty(),
                comparison.mismatches.isEmpty() ? "OK" : "Mismatches found",
                expectedBalances.size(),
                comparison.matchCount,
                comparison.mismatches.size()));

        // Report results
        if (comparison.mismatches.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|green ✓ All " + expectedBalances.size() + " accounts match|@"));
        } else {
            System.out.println(Ansi.AUTO.string("@|red ✗ Found " + comparison.mismatches.size() + " mismatches out of "
                    + expectedBalances.size() + " accounts|@"));
            // Print first 10 mismatches
            int shown = 0;
            for (Map.Entry<Long, BalanceMismatch> entry : comparison.mismatches.entrySet()) {
                if (shown++ >= 10) {
                    System.out.println(
                            Ansi.AUTO.string("  @|yellow ... and " + (comparison.mismatches.size() - 10) + " more|@"));
                    break;
                }
                BalanceMismatch m = entry.getValue();
                System.out.println(Ansi.AUTO.string(String.format(
                        "  Account @|cyan %d|@: expected @|yellow %,d|@ but computed @|red %,d|@ (diff: @|red %+,d|@)",
                        entry.getKey(), m.expected, m.computed, m.computed - m.expected)));
            }
            throw new ValidationException("Balance validation failed at block " + checkpointBlock + ": "
                    + comparison.mismatches.size() + " mismatches");
        }
    }

    /**
     * Compare expected balances with computed balances.
     */
    private ComparisonResult compareBalances(Map<Long, Long> expectedBalances, Map<Long, Long> computedBalances) {
        Map<Long, BalanceMismatch> mismatches = new TreeMap<>();
        int matchCount = 0;

        for (Map.Entry<Long, Long> entry : expectedBalances.entrySet()) {
            long accountId = entry.getKey();
            long expected = entry.getValue();
            Long computed = computedBalances.get(accountId);

            if (computed == null) {
                mismatches.put(accountId, new BalanceMismatch(expected, 0L));
            } else if (!computed.equals(expected)) {
                mismatches.put(accountId, new BalanceMismatch(expected, computed));
            } else {
                matchCount++;
            }
        }

        return new ComparisonResult(matchCount, mismatches);
    }

    /**
     * Print a summary of all checkpoint validations.
     */
    public void printSummary() {
        if (results.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow No balance checkpoints were validated|@"));
            return;
        }

        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   BALANCE CHECKPOINT VALIDATION SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));

        long passed = results.stream().filter(r -> r.passed).count();
        long failed = results.size() - passed;

        System.out.println(Ansi.AUTO.string("@|yellow Checkpoints validated:|@ " + results.size()));
        System.out.println(Ansi.AUTO.string("@|green Passed:|@ " + passed));
        if (failed > 0) {
            System.out.println(Ansi.AUTO.string("@|red Failed:|@ " + failed));
            System.out.println(Ansi.AUTO.string("\n@|red Failed checkpoints:|@"));
            for (CheckpointResult r : results) {
                if (!r.passed) {
                    System.out.println(Ansi.AUTO.string("  @|red Block " + r.blockNumber + "|@: " + r.message + " ("
                            + r.mismatchCount + " mismatches)"));
                }
            }
        }
    }

    /**
     * Check if all validations passed.
     */
    public boolean allPassed() {
        return results.stream().allMatch(r -> r.passed);
    }

    /**
     * Get the number of loaded checkpoints.
     */
    public int getCheckpointCount() {
        return loader.getCheckpointCount();
    }

    /**
     * Check if checkpoints are available within the given block range.
     *
     * @param startBlock the start block number
     * @param endBlock the end block number
     * @return true if at least one checkpoint exists in the range
     */
    public boolean hasCheckpointsInRange(long startBlock, long endBlock) {
        if (sortedCheckpointBlocks == null || sortedCheckpointBlocks.isEmpty()) {
            return false;
        }
        for (long block : sortedCheckpointBlocks) {
            if (block >= startBlock && block <= endBlock) {
                return true;
            }
            if (block > endBlock) {
                break;
            }
        }
        return false;
    }

    /** Result of a single checkpoint validation */
    public record CheckpointResult(
            long blockNumber, boolean passed, String message, int totalAccounts, int matchCount, int mismatchCount) {}

    /** A balance mismatch between expected and computed values */
    private record BalanceMismatch(long expected, long computed) {}

    /** Result of comparing balances */
    private record ComparisonResult(int matchCount, Map<Long, BalanceMismatch> mismatches) {}
}
