// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import static org.hiero.block.tools.mirrornode.model.MirrorNodeTransaction.TRANSACTION_1;
import static org.hiero.block.tools.mirrornode.model.MirrorNodeTransaction.TRANSACTION_2;
import static org.hiero.block.tools.mirrornode.model.MirrorNodeTransaction.TRANSACTION_3;
import static org.hiero.block.tools.states.SavedStateConverter.convertStateToStateChanges;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hiero.block.tools.mirrornode.model.MirrorNodeTransaction;
import org.hiero.block.tools.states.balances.CsvAccountBalances;
import org.hiero.block.tools.states.model.SignedState;
import picocli.CommandLine.Command;

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
@Command(
        name = "oa-state",
        description = "Load and print the state at the start of Hedera Mainnet Open Access (round 33485415)")
public class MainnetOAState implements Runnable {
    /** Directory containing the saved state at the start of Hedera Mainnet Open Access (round 33485415) */
    public static String STATE_33485415_DIR_URL = "/saved-state-33485415";
    /** CSV file containing balances at the time of the saved state */
    public static URL BALANCES_CSV_2019_09_13T22_URL =
            MainnetOAState.class.getResource("/2019-09-13T22_00_00.000081Z_Balances.csv.gz");

    /** Load the state at the start of Hedera Mainnet Open Access (round 33485415) */
    private static List<BlockItem> load33485415State() {
        return convertStateToStateChanges(STATE_33485415_DIR_URL);
    }

    /** Holds balances from the CSV file for the date 2019-09-13T22:00:00.000081Z */
    private static Map<Long, Long> loadAccountBalancesCsv() {
        return CsvAccountBalances.loadCsvBalances(BALANCES_CSV_2019_09_13T22_URL);
    }

    /**
     * Reverses the balance changes from the given transactions on the original balances.
     *
     * @param originalBalances the original balances map where the key is the account ID and the value is the balance
     * @param transactions the transactions to reverse, where each transaction contains transfers
     * @return a new map with the reversed balances
     */
    public static Map<Long, Long> reverseTransactions(
            Map<Long, Long> originalBalances, MirrorNodeTransaction... transactions) {
        return originalBalances.entrySet().stream()
                .map(entry -> {
                    final long accountId = entry.getKey();
                    long balance = entry.getValue();
                    for (MirrorNodeTransaction transaction : transactions) {
                        // reverse the balance changes from each transaction
                        for (long[] transfer : transaction.transfers()) {
                            if (transfer[0] == accountId) {
                                // reverse the balance
                                balance -= transfer[1];
                            }
                        }
                    }
                    return Map.entry(accountId, balance);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Compares two maps of account balances and prints the differences.
     *
     * @param balances1 the first map of account balances
     * @param balances1Name the name of the first map (for printing)
     * @param balances2 the second map of account balances
     * @param balances2Name the name of the second map (for printing)
     */
    public static void compareAccounts(
            Map<Long, Long> balances1, String balances1Name, Map<Long, Long> balances2, String balances2Name) {
        // compare the two maps, finding accounts that are in the state but not in the CSV and vice versa
        System.out.println("\n===========================================================================");
        System.out.println("Comparing balances from " + balances1Name + " and " + balances2Name + "...");
        // Accounts in state but not in CSV
        Map<Long, Long> missingInCsv = balances1.entrySet().stream()
                .filter(entry -> !balances2.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // Accounts in CSV but not in state
        Map<Long, Long> missingInState = balances2.entrySet().stream()
                .filter(entry -> !balances1.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // find accounts with different balances
        Map<Long, Long> differentBalances = balances1.entrySet().stream()
                .filter(entry -> balances2.containsKey(entry.getKey())
                        && !entry.getValue().equals(balances2.get(entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // print results
        System.out.println("    Accounts in state but not in CSV: " + missingInCsv.size());
        missingInCsv.forEach((accountId, balance) ->
                System.out.println("        Account ID: " + accountId + ", Balance: " + balance));
        System.out.println("    Accounts in CSV but not in state: " + missingInState.size());
        missingInState.forEach((accountId, balance) ->
                System.out.println("        Account ID: " + accountId + ", Balance: " + balance));
        System.out.println("    Accounts with different balances: " + differentBalances.size());
        differentBalances.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            long accountId = entry.getKey();
            long balance = entry.getValue();
            long balance2 = balances2.get(accountId);
            System.out.printf(
                    "        Account ID: %8d, State Balance: %,18d, CSV Balance: %,18d (Difference: %,12d)\n",
                    accountId, balance, balance2, (balance - balance2));
        });
    }

    /**
     * Extracts account balances from a SignedState object and returns them as a map.
     *
     * @param signedState the SignedState object containing the account balances
     * @return a map where the key is the account ID and the value is the balance
     */
    public static Map<Long, Long> getBalancesFromSignedState(SignedState signedState) {
        // convert state balances to a map
        return signedState.state().accountMap().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().accountId(), entry -> entry.getValue()
                        .balance()));
    }

    @Override
    public void run() {
        try {
            System.out.println(Block.JSON.toJSON(new Block(load33485415State())));


            var BALANCES_CSV_2019_09_13T22 = MainnetOAState.loadAccountBalancesCsv();
            SignedState signedState33485415 = loadSignedStateForRound(33485415);
            SignedState signedState33486127 = loadSignedStateForRound(33486127);
            // convert state balances to a maps
            Map<Long, Long> state33485415Balances = MainnetOAState.getBalancesFromSignedState(signedState33485415);
            Map<Long, Long> state33486127Balances = MainnetOAState.getBalancesFromSignedState(signedState33486127);
            // compare rounds and CVV
            MainnetOAState.compareAccounts(
                state33485415Balances,
                "State " + 33485415,
                BALANCES_CSV_2019_09_13T22,
                "CSV BALANCES_CSV_2019_09_13T22");
            MainnetOAState.compareAccounts(
                state33485415Balances, "State " + 33485415, state33486127Balances, "State " + 33486127);
            MainnetOAState.compareAccounts(
                state33486127Balances,
                "State " + 33486127,
                BALANCES_CSV_2019_09_13T22,
                "CSV BALANCES_CSV_2019_09_13T22");

            // print transaction transfers
            System.out.println(TRANSACTION_1);
            System.out.println(TRANSACTION_2);
            System.out.println(TRANSACTION_3);
            // now lets take state33486127Balances reverse balance changes from transactions 2 and 3
            // this should give us the same state to what we had in state33485415Balances
            Map<Long, Long> state33486127BalancesReversed =
                reverseTransactions(state33486127Balances, TRANSACTION_2, TRANSACTION_3);
            // compare state33486127BalancesReversed with signedState33485415
            MainnetOAState.compareAccounts(
                state33485415Balances,
                "State 33485415",
                state33486127BalancesReversed,
                "state33486127BalancesReversed");
            // now let's take state33485415Balances and reverse balance changes from transaction 1
            // this should give us the balances at the beginning of the network
            Map<Long, Long> initialBalances = reverseTransactions(state33485415Balances, TRANSACTION_1);
            // compare initialBalances with CSV balances
            MainnetOAState.compareAccounts(
                initialBalances, "Initial Balances", BALANCES_CSV_2019_09_13T22, "CSV BALANCES_CSV_2019_09_13T22");


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
