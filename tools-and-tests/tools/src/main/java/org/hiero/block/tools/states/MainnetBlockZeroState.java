// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import static org.hiero.block.tools.states.SavedStateConverter.loadState;

import com.hedera.hapi.block.stream.BlockItem;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hiero.block.tools.mirrornode.model.MirrorNodeTransaction;
import org.hiero.block.tools.states.balances.CsvAccountBalances;
import org.hiero.block.tools.states.model.CompleteSavedState;
import org.hiero.block.tools.states.model.FCMap;
import org.hiero.block.tools.states.model.MapKey;
import org.hiero.block.tools.states.model.MapValue;
import org.hiero.block.tools.states.model.StorageKey;
import org.hiero.block.tools.states.model.StorageValue;
import org.hiero.block.tools.states.postgres.BlobType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;

/**
 * Subcommand to construct and verify the state at beginning of block zero of Hedera mainnet. Block zero is what we
 * call "Stream Start" and happened on 13th September 2019. It was the beginning of the Hedera mainnet blockchain.
 * Unfortunately the state of the network was not empty at stream start, there were accounts, files and Smart Contract
 * KV pairs that existed. The closest historical data that was stored is a saved state snapshot taken at round 33485415,
 * which is at the end of block zero. So the result is the only way to get a state at beginning of block zero is to take
 * that snapshot and reverse the 1 transaction that happened in block zero. Luckily that transaction was only simple
 * crypto transfer, and it is known and available from the Hedera Mirror Node.
 *
 * <pre>
 * Event                              Consensus Timestamp      Consensus Time UTC           0.0.98 balance
 * ════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 * Block 0                            1568411631.396440000     2019-09-13T21:53:51.396440Z
 * └─ Txn 0.0.11337@1568411616.448357000 <a href="https://hashscan.io/mainnet/transaction/1568411631.396440000">t1</a>
 *                                    1568411631.396440000     2019-09-13T21:53:51.396440Z  +83,417
 * Saved State 33485415               1568411631.916679000     2019-09-13T21:53:51.916679Z  982,540,799,613
 *
 * Block 1                            1568411670.872035001     2019-09-13T21:54:30.872035Z
 * └─ Txn 0.0.11337@1568411656.265684000 <a href="https://hashscan.io/mainnet/transaction/1568411670.872035001">t2</a>
 *                                    1568411670.872035001     2019-09-13T21:54:30.872035Z  +83,417
 *
 * Block 2                            1568411762.486929000     2019-09-13T21:56:02.486929Z
 * └─ Txn 0.0.11337@1568411747.660028000 <a href="https://hashscan.io/mainnet/transaction/1568411762.4869290001">t3</a>
 *
 * 2019-09-13T22:00:00 Balances.csv   1568412000.81000         2019-09-13T22:00:00.000081Z  982,540,966,447
 * Saved State 33486127               1568412000.81000         2019-09-13T22:00:00.000081Z  982,540,966,447
 *
 * Block 3                            1568412919.286477002     2019-09-13T22:15:19.286477Z
 * └─ Txn 0.0.14622@1568412908.640207071 <a href="https://hashscan.io/mainnet/transaction/1568412919.286477002">t4</a>
 *                                    1568412919.286477002     2019-09-13T22:15:19.286477Z
 * </pre>
 */
@SuppressWarnings({"DuplicatedCode", "unused"})
@Command(name = "block-zero", description = "Load and validate the state at the start of Hedera Mainnet Block Zero")
public class MainnetBlockZeroState implements Runnable {
    /** Directory containing the saved state at the start of Hedera Mainnet Open Access (round 33485415) */
    public static String STATE_33485415_DIR_URL = "/saved-state-33485415";
    /** CSV file containing balances at the time of the saved state */
    public static URL BALANCES_CSV_2019_09_13T22_URL =
            MainnetBlockZeroState.class.getResource("/2019-09-13T22_00_00.000081Z_Balances.csv.gz");

    /** Cached state at the start of Hedera Mainnet Open Access (round 33485415) */
    private static CompleteSavedState state33485415;
    /** Cached state at the start of Hedera Mainnet at block zero */
    private static CompleteSavedState blockZeroState;

    /** Load the state at the start of Hedera Mainnet Open Access (round 33485415) */
    public static CompleteSavedState load33485415State() {
        if (state33485415 == null) {
            state33485415 = loadState(STATE_33485415_DIR_URL);
        }
        return state33485415;
    }

    /** Holds balances from the CSV file for the date 2019-09-13T22:00:00.000081Z */
    public static Map<Long, Long> loadAccountBalancesCsv2019Sep13() {
        return CsvAccountBalances.loadCsvBalances(BALANCES_CSV_2019_09_13T22_URL);
    }

    /**
     * Get the block items representing the state changes for the initial state at the start of Hedera Mainnet at the
     * beginning of block zero.
     *
     * @return the complete saved state at the start of block zero
     */
    public static CompleteSavedState loadStartBlockZeroState() {
        if (blockZeroState == null) {
            final CompleteSavedState stateEndBlockZero = load33485415State();
            blockZeroState = reverseTransactions(stateEndBlockZero, MirrorNodeTransaction.getTransaction1());
        }
        return blockZeroState;
    }

    /**
     * Get the block items representing the state changes for the initial state at the start of Hedera Mainnet at the
     * beginning of block zero.
     *
     * @return list of block items representing the state changes at the start of block zero
     */
    public static List<BlockItem> loadStartBlockZeroStateChanges() {
        return SavedStateConverter.signedStateToStateChanges(loadStartBlockZeroState());
    }

    /**
     * Reverses the balance changes from the given transactions on the original state.
     *
     * @param state the original state
     * @param transactions the transactions to reverse, where each transaction contains transfers
     * @return a new map with the reversed balances
     */
    public static CompleteSavedState reverseTransactions(
            CompleteSavedState state, MirrorNodeTransaction... transactions) {
        // Build a map of accountId -> total amount to reverse across all transactions
        Map<Long, Long> reversals = new HashMap<>();
        for (MirrorNodeTransaction transaction : transactions) {
            for (long[] transfer : transaction.transfers()) {
                reversals.merge(transfer[0], transfer[1], Long::sum);
            }
        }
        // Apply reversals to the account map
        FCMap<MapKey, MapValue> accountMap = state.signedState().state().accountMap();
        for (Map.Entry<MapKey, MapValue> entry : accountMap.entrySet()) {
            Long reversal = reversals.get(entry.getKey().accountId());
            if (reversal != null) {
                MapValue oldValue = entry.getValue();
                entry.setValue(new MapValue(
                        oldValue.balance() - reversal,
                        oldValue.receiverThreshold(),
                        oldValue.senderThreshold(),
                        oldValue.receiverSigRequired(),
                        oldValue.accountKeys(),
                        oldValue.proxyAccount(),
                        oldValue.autoRenewPeriod(),
                        oldValue.deleted(),
                        oldValue.recordLinkedList(),
                        oldValue.expirationTime(),
                        oldValue.memo(),
                        oldValue.isSmartContract()));
            }
        }
        return state;
    }

    /**
     * Applies the balance changes from the given transactions on the original state.
     *
     * @param state the original state
     * @param transactions the transactions to apply, where each transaction contains transfers
     * @return a new map with the reversed balances
     */
    public static CompleteSavedState applyTransactions(
            CompleteSavedState state, MirrorNodeTransaction... transactions) {
        // Build a map of accountId -> total amount to apply across all transactions
        Map<Long, Long> adjustments = new HashMap<>();
        for (MirrorNodeTransaction transaction : transactions) {
            for (long[] transfer : transaction.transfers()) {
                adjustments.merge(transfer[0], transfer[1], Long::sum);
            }
        }
        // Apply adjustments to the account map
        FCMap<MapKey, MapValue> accountMap = state.signedState().state().accountMap();
        for (Map.Entry<MapKey, MapValue> entry : accountMap.entrySet()) {
            Long adjustment = adjustments.get(entry.getKey().accountId());
            if (adjustment != null) {
                MapValue oldValue = entry.getValue();
                entry.setValue(new MapValue(
                        oldValue.balance() + adjustment,
                        oldValue.receiverThreshold(),
                        oldValue.senderThreshold(),
                        oldValue.receiverSigRequired(),
                        oldValue.accountKeys(),
                        oldValue.proxyAccount(),
                        oldValue.autoRenewPeriod(),
                        oldValue.deleted(),
                        oldValue.recordLinkedList(),
                        oldValue.expirationTime(),
                        oldValue.memo(),
                        oldValue.isSmartContract()));
            }
        }
        return state;
    }

    /**
     * Compares two maps of account balances and prints the differences.
     *
     * @param expectedBalances the map of expected account balances
     * @param expectedBalancesName the name of the expected map (for printing)
     * @param comparingBalances the map of account balances to compare
     * @param comparingBalancesName the name of the comparing map (for printing)
     */
    public static void compareAccounts(
            Map<Long, Long> expectedBalances,
            String expectedBalancesName,
            Map<Long, Long> comparingBalances,
            String comparingBalancesName) {
        // compare the two maps
        Map<Long, Long> onlyInExpected = expectedBalances.entrySet().stream()
                .filter(entry -> !comparingBalances.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Long, Long> onlyInComparing = comparingBalances.entrySet().stream()
                .filter(entry -> !expectedBalances.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Long, Long> differentBalances = expectedBalances.entrySet().stream()
                .filter(entry -> comparingBalances.containsKey(entry.getKey())
                        && !entry.getValue().equals(comparingBalances.get(entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        long matchingCount = expectedBalances.size() - onlyInExpected.size() - differentBalances.size();

        // print header
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   COMPARE ACCOUNTS|@"));
        System.out.println(Ansi.AUTO.string(
                String.format("  @|bold %s|@  vs  @|bold %s|@", expectedBalancesName, comparingBalancesName)));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));

        // summary counts
        System.out.println(Ansi.AUTO.string(String.format("  Matching accounts:    @|green %,d|@", matchingCount)));
        System.out.println(Ansi.AUTO.string(String.format(
                "  Different balances:   @|%s %,d|@",
                differentBalances.isEmpty() ? "green" : "red", differentBalances.size())));
        System.out.println(Ansi.AUTO.string(String.format(
                "  Only in expected:     @|%s %,d|@",
                onlyInExpected.isEmpty() ? "green" : "yellow", onlyInExpected.size())));
        System.out.println(Ansi.AUTO.string(String.format(
                "  Only in comparing:    @|%s %,d|@",
                onlyInComparing.isEmpty() ? "green" : "yellow", onlyInComparing.size())));

        // accounts only in expected
        if (!onlyInExpected.isEmpty()) {
            System.out.println(Ansi.AUTO.string("\n@|bold,yellow   ▶ Accounts only in " + expectedBalancesName + "|@"));
            System.out.println(Ansi.AUTO.string("@|yellow   ──────────────────────────────────────────────|@"));
            onlyInExpected.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> System.out.println(Ansi.AUTO.string(String.format(
                            "    Account @|cyan %8d|@  Balance: @|yellow %,18d|@", entry.getKey(), entry.getValue()))));
        }

        // accounts only in comparing
        if (!onlyInComparing.isEmpty()) {
            System.out.println(
                    Ansi.AUTO.string("\n@|bold,yellow   ▶ Accounts only in " + comparingBalancesName + "|@"));
            System.out.println(Ansi.AUTO.string("@|yellow   ──────────────────────────────────────────────|@"));
            onlyInComparing.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> System.out.println(Ansi.AUTO.string(String.format(
                            "    Account @|cyan %8d|@  Balance: @|yellow %,18d|@", entry.getKey(), entry.getValue()))));
        }

        // accounts with different balances
        if (!differentBalances.isEmpty()) {
            System.out.println(Ansi.AUTO.string("\n@|bold,red   ▶ Accounts with different balances|@"));
            System.out.println(Ansi.AUTO.string("@|red   ──────────────────────────────────────────────|@"));
            differentBalances.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        long accountId = entry.getKey();
                        long expected = entry.getValue();
                        long comparing = comparingBalances.get(accountId);
                        long diff = expected - comparing;
                        System.out.println(Ansi.AUTO.string(String.format(
                                "    Account @|cyan %8d|@  Expected: @|yellow %,18d|@  Comparing: @|yellow %,18d|@  Diff: @|red %,+14d|@",
                                accountId, expected, comparing, diff)));
                    });
        }

        // overall result
        if (onlyInExpected.isEmpty() && onlyInComparing.isEmpty() && differentBalances.isEmpty()) {
            System.out.println(Ansi.AUTO.string("\n  @|bold,green ✓ All accounts match perfectly|@"));
        } else {
            System.out.println(Ansi.AUTO.string("\n  @|bold,red ✗ Differences found|@"));
        }
        System.out.println(Ansi.AUTO.string("@|blue ────────────────────────────────────────────────────────────|@"));
    }

    /**
     * Extracts account balances from a SignedState object and returns them as a map.
     *
     * @param savedState the CompleteSavedState object containing the account balances
     * @return a map where the key is the account ID and the value is the balance
     */
    public static Map<Long, Long> getBalancesFromSignedState(CompleteSavedState savedState) {
        // convert state balances to a map
        return savedState.signedState().state().accountMap().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().accountId(), entry -> entry.getValue()
                        .balance()));
    }

    /**
     * Prints a summary of the complete saved state. This includes key statistics such as:
     * - round
     * - consensusTimestamp
     * - number of accounts: HGCAppState.accountMap.size()
     * - total balance of all accounts
     * - number of binary object files: CompleteSavedState.binaryObjectByHexHashMap.size()
     * - number of smart contracts KV pairs: HGCAppState.storageMap.size()
     *
     * @param state the CompleteSavedState object to summarize
     * @param stateName the name of the state (for printing)
     */
    public static void printCompleteSavedStateSummary(CompleteSavedState state, String stateName) {
        final var signedState = state.signedState();
        final var accountMap = signedState.state().accountMap();
        final FCMap<StorageKey, StorageValue> storageMap = signedState.state().storageMap();
        final long totalBalance =
                accountMap.values().stream().mapToLong(MapValue::balance).sum();
        final long numAccounts = accountMap.size();
        final long numSmartContracts =
                accountMap.values().stream().filter(MapValue::isSmartContract).count();
        final long numBinaryObjects = state.binaryObjectByHexHashMap().size();
        final long numStorageEntries = storageMap.size();

        // Count storage entries by blob type
        Map<BlobType, Long> blobTypeCounts = storageMap.keySet().stream()
                .collect(Collectors.groupingBy(StorageKey::getBlobType, Collectors.counting()));
        final long numFiles = blobTypeCounts.getOrDefault(BlobType.FILE_METADATA, 0L);
        final long numContractBytecodes = blobTypeCounts.getOrDefault(BlobType.CONTRACT_BYTECODE, 0L);
        final long numContractStorageEntries = blobTypeCounts.getOrDefault(BlobType.CONTRACT_STORAGE, 0L);

        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   SAVED STATE SUMMARY: " + stateName + "|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(
                Ansi.AUTO.string(String.format("  Round:                @|yellow %,d|@", signedState.round())));
        System.out.println(Ansi.AUTO.string(
                String.format("  Consensus Timestamp:  @|cyan %s|@", signedState.consensusTimestamp())));
        System.out.println(Ansi.AUTO.string(String.format(
                "  Accounts:             @|yellow %,d|@ (@|yellow %,d|@ normal + @|yellow %,d|@ smart contracts)",
                numAccounts, numAccounts - numSmartContracts, numSmartContracts)));
        System.out.println(
                Ansi.AUTO.string(String.format("  Total Balance:        @|yellow %,d|@ tinybars", totalBalance)));
        System.out.println(Ansi.AUTO.string(String.format("  Files:                @|yellow %,d|@", numFiles)));
        System.out.println(
                Ansi.AUTO.string(String.format("  Contract Bytecodes:   @|yellow %,d|@", numContractBytecodes)));
        System.out.println(
                Ansi.AUTO.string(String.format("  Contract Storage:     @|yellow %,d|@", numContractStorageEntries)));
        System.out.println(Ansi.AUTO.string(String.format("  Binary Objects:       @|yellow %,d|@", numBinaryObjects)));
        System.out.println(
                Ansi.AUTO.string(String.format("  Storage KV Pairs:     @|yellow %,d|@", numStorageEntries)));
        System.out.println(Ansi.AUTO.string("@|blue ────────────────────────────────────────────────────────────|@"));
    }

    @Override
    public void run() {
        try {
            // load the state at start of block zero
            final CompleteSavedState stateStartBlockZero = loadStartBlockZeroState();
            printCompleteSavedStateSummary(stateStartBlockZero, "Start of Block 0 State");
            // apply the transaction changes from block zero
            final CompleteSavedState computedStateEndBlockZero =
                    applyTransactions(stateStartBlockZero, MirrorNodeTransaction.getTransaction1());
            final Map<Long, Long> computedStateEndBlockZeroBalances =
                    getBalancesFromSignedState(computedStateEndBlockZero);
            printCompleteSavedStateSummary(computedStateEndBlockZero, "Computed End of Block 0 State");
            // compare with saved state at end of block zero 33485415
            final CompleteSavedState stateEndBlockZero = load33485415State();
            stateEndBlockZero.printValidationReport();
            final Map<Long, Long> stateEndBlockZeroBalances = getBalancesFromSignedState(stateEndBlockZero);
            // compare the balances of the two states
            compareAccounts(
                    stateEndBlockZeroBalances,
                    "Loaded end of block zero 33485415",
                    computedStateEndBlockZeroBalances,
                    "Computed end of block zero after applying tx1");
            // apply transaction 2 & 3 changes to get end of block two state
            final CompleteSavedState computedStateEndBlockTwo = applyTransactions(
                    computedStateEndBlockZero,
                    MirrorNodeTransaction.getTransaction2(),
                    MirrorNodeTransaction.getTransaction3());
            final Map<Long, Long> computedStateEndBlockTwoBalances =
                    getBalancesFromSignedState(computedStateEndBlockTwo);
            // load the balances CSV at 2019-09-13T22:00:00.000081Z which is end of block two
            final Map<Long, Long> csvBalances2019_09_13T22 = loadAccountBalancesCsv2019Sep13();
            // compare with CSV balances at 2019-09-13T22:00:00.000081Z
            compareAccounts(
                    csvBalances2019_09_13T22,
                    "CSV Balances at 2019-09-13T22:00:00.000081Z",
                    computedStateEndBlockTwoBalances,
                    "Computed end of block two after applying tx2 & tx3");
        } catch (Exception e) {
            throw new UncheckedIOException(
                    new java.io.IOException("Failed to construct and validate block zero state", e));
        }
    }
}
