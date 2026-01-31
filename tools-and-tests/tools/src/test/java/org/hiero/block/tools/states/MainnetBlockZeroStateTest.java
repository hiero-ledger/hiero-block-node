// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.tools.states.model.CompleteSavedState;
import org.junit.jupiter.api.Test;

/** Tests for {@link MainnetBlockZeroState}. */
class MainnetBlockZeroStateTest {

    @Test
    void loadOaStateReturnsNonEmptyList() {
        CompleteSavedState completeSavedState = assertDoesNotThrow(MainnetBlockZeroState::load33485415State);
        assertNotNull(completeSavedState);
        List<BlockItem> blockItems = SavedStateConverter.signedStateToStateChanges(completeSavedState);
        assertFalse(blockItems.isEmpty(), "loadOaState should return a non-empty list of block items");
    }

    @Test
    void getBalancesFromSignedState() {
        CompleteSavedState state = MainnetBlockZeroState.load33485415State();
        Map<Long, Long> balances = MainnetBlockZeroState.getBalancesFromSignedState(state);
        assertNotNull(balances);
        assertFalse(balances.isEmpty());
        // Account 0.0.98 (node fee account) should exist
        assertTrue(balances.containsKey(98L));
    }

    @Test
    void printCompleteSavedStateSummary() {
        CompleteSavedState state = MainnetBlockZeroState.load33485415State();
        assertDoesNotThrow(() -> MainnetBlockZeroState.printCompleteSavedStateSummary(state, "Test State"));
    }

    @Test
    void printValidationReport() {
        CompleteSavedState state = MainnetBlockZeroState.load33485415State();
        assertDoesNotThrow(state::printValidationReport);
    }

    @Test
    void compareAccountsIdentical() {
        Map<Long, Long> balances = new HashMap<>();
        balances.put(1L, 100L);
        balances.put(2L, 200L);
        // Should not throw
        assertDoesNotThrow(() ->
                MainnetBlockZeroState.compareAccounts(balances, "Expected", new HashMap<>(balances), "Comparing"));
    }

    @Test
    void compareAccountsDifferent() {
        Map<Long, Long> expected = new HashMap<>();
        expected.put(1L, 100L);
        expected.put(2L, 200L);
        expected.put(3L, 300L);

        Map<Long, Long> comparing = new HashMap<>();
        comparing.put(1L, 100L);
        comparing.put(2L, 999L); // different
        comparing.put(4L, 400L); // only in comparing

        assertDoesNotThrow(() -> MainnetBlockZeroState.compareAccounts(expected, "Expected", comparing, "Comparing"));
    }

    @Test
    void loadAccountBalancesCsv2019_09_13T22() {
        Map<Long, Long> balances = MainnetBlockZeroState.loadAccountBalancesCsv2019_09_13T22();
        assertNotNull(balances);
        assertFalse(balances.isEmpty());
    }
}
