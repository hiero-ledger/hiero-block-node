package org.hiero.block.tools.states.utils;

import org.hiero.block.tools.states.MirrorNodeTransaction;
import org.hiero.block.tools.states.model.SignedState;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for handling account balances in Hedera Signed States and transactions.
 * Provides methods to reverse transactions, compare balances, and extract balances from signed states.
 */
public class BalancesTools {
    /**
     * Reverses the balance changes from the given transactions on the original balances.
     *
     * @param originalBalances the original balances map where the key is the account ID and the value is the balance
     * @param transactions the transactions to reverse, where each transaction contains transfers
     * @return a new map with the reversed balances
     */
    public static Map<Long,Long> reverseTransactions(Map<Long,Long> originalBalances, MirrorNodeTransaction... transactions) {
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
    public static void compareAccounts(Map<Long, Long> balances1, String balances1Name, Map<Long, Long> balances2,
            String balances2Name) {
        // compare the two maps, finding accounts that are in the state but not in the CSV and vice versa
        System.out.println("\n===========================================================================");
        System.out.println("Comparing balances from "+balances1Name+" and "+balances2Name+"...");
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
                .filter(entry -> balances2.containsKey(entry.getKey()) && !entry.getValue().equals(
                        balances2.get(entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // print results
        System.out.println("    Accounts in state but not in CSV: " + missingInCsv.size());
        missingInCsv.forEach((accountId, balance) ->
            System.out.println("        Account ID: " + accountId + ", Balance: " + balance));
        System.out.println("    Accounts in CSV but not in state: " + missingInState.size());
        missingInState.forEach((accountId, balance) ->
            System.out.println("        Account ID: " + accountId + ", Balance: " + balance));
        System.out.println("    Accounts with different balances: " + differentBalances.size());
        differentBalances.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    long accountId = entry.getKey();
                    long balance = entry.getValue();
                    long balance2 = balances2.get(accountId);
                    System.out.printf(
                        "        Account ID: %8d, State Balance: %,18d, CSV Balance: %,18d (Difference: %,12d)\n",
                        accountId, balance, balance2, (balance - balance2)
                    );
                });
    }

    /**
     * Extracts account balances from a SignedState object and returns them as a map.
     *
     * @param signedState the SignedState object containing the account balances
     * @return a map where the key is the account ID and the value is the balance
     */
    public static  Map<Long, Long> getBalancesFromSignedState(SignedState signedState) {
        // convert state balances to a map
        return signedState.state().accountMap().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().accountId(),
                        entry -> entry.getValue().balance()
                ));
    }
}
