package org.hiero.block.tools.states;

import static org.hiero.block.tools.states.BalancesTools.reverseTransactions;
import static org.hiero.block.tools.states.CsvBalances.BALANCES_CSV_2019_09_13T22;
import static org.hiero.block.tools.states.Main.loadSignedState;
import static org.hiero.block.tools.states.MirrorNodeTransaction.TRANSACTION_1;
import static org.hiero.block.tools.states.MirrorNodeTransaction.TRANSACTION_2;
import static org.hiero.block.tools.states.MirrorNodeTransaction.TRANSACTION_3;

import org.hiero.block.tools.states.model.SignedState;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class BalancesComparisonApp {

    public static SignedState loadSignedStateForRound(long round) throws IOException {
        final Path stateFile = Path.of("mainnet-data/"+round+"/SignedState.swh");
        return loadSignedState(stateFile);
    }

    public static void main(String[] args) throws IOException {
        SignedState signedState33485415 = loadSignedStateForRound(33485415);
        SignedState signedState33486127 = loadSignedStateForRound(33486127);
        // convert state balances to a maps
        Map<Long, Long> state33485415Balances = BalancesTools.getBalancesFromSignedState(signedState33485415);
        Map<Long, Long> state33486127Balances = BalancesTools.getBalancesFromSignedState(signedState33486127);
        // compare rounds and CVV
        BalancesTools.compareAccounts(state33485415Balances, "State "+33485415,
                BALANCES_CSV_2019_09_13T22, "CSV BALANCES_CSV_2019_09_13T22");
        BalancesTools.compareAccounts(state33485415Balances, "State "+33485415,
                state33486127Balances, "State "+33486127);
        BalancesTools.compareAccounts(state33486127Balances, "State "+33486127,
                BALANCES_CSV_2019_09_13T22, "CSV BALANCES_CSV_2019_09_13T22");

        // print transaction transfers
        System.out.println(TRANSACTION_1);
        System.out.println(TRANSACTION_2);
        System.out.println(TRANSACTION_3);
        // now lets take state33486127Balances reverse balance changes from transactions 2 and 3
        // this should give us the same state to what we had in state33485415Balances
        Map<Long, Long> state33486127BalancesReversed =
                reverseTransactions(state33486127Balances, TRANSACTION_2, TRANSACTION_3);
        // compare state33486127BalancesReversed with signedState33485415
        BalancesTools.compareAccounts(state33485415Balances, "State 33485415",
                state33486127BalancesReversed, "state33486127BalancesReversed");
        // now let's take state33485415Balances and reverse balance changes from transaction 1
        // this should give us the balances at the beginning of the network
        Map<Long, Long> initialBalances = reverseTransactions(state33485415Balances, TRANSACTION_1);
        // compare initialBalances with CSV balances
        BalancesTools.compareAccounts(initialBalances, "Initial Balances",
                BALANCES_CSV_2019_09_13T22, "CSV BALANCES_CSV_2019_09_13T22");
    }


}
