// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MirrorNodeTransaction}.
 *
 * <p>Expected transfer data is derived from the Hedera mainnet mirror node API responses for each
 * transaction. The code under test sums duplicate account entries, so the expected values below are
 * the per-account totals.
 *
 * <p>Transaction 1 &amp; 2 transfers (accounts 3, 98, 11337):
 * <ul>
 *   <li>0.0.3: 1788 + 100000 = 101788</li>
 *   <li>0.0.98: 880 + 82537 = 83417</li>
 *   <li>0.0.11337: -100000 + -84325 + -880 = -185205</li>
 * </ul>
 *
 * <p>Transaction 3 transfers (accounts 12, 98, 11337):
 * <ul>
 *   <li>0.0.12: 1788 + 100000 = 101788</li>
 *   <li>0.0.98: 880 + 82537 = 83417</li>
 *   <li>0.0.11337: -100000 + -84325 + -880 = -185205</li>
 * </ul>
 */
class MirrorNodeTransactionTest {

    // ==================== getTransaction1 ====================

    @Test
    void transaction1HasCorrectIdAndNickname() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction1();
        assertEquals("0.0.11337-1568411616-448357000", tx.transactionId());
        assertEquals("Transaction 1", tx.nickname());
    }

    @Test
    void transaction1HasCorrectTransfers() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction1();
        Map<Long, Long> expected = Map.of(
                3L, 101788L,
                98L, 83417L,
                11337L, -185205L);
        assertTransfersMatch(expected, tx.transfers());
    }

    // ==================== getTransaction2 ====================

    @Test
    void transaction2HasCorrectIdAndNickname() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction2();
        assertEquals("0.0.11337-1568411656-265684000", tx.transactionId());
        assertEquals("Transaction 2", tx.nickname());
    }

    @Test
    void transaction2HasCorrectTransfers() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction2();
        Map<Long, Long> expected = Map.of(
                3L, 101788L,
                98L, 83417L,
                11337L, -185205L);
        assertTransfersMatch(expected, tx.transfers());
    }

    // ==================== getTransaction3 ====================

    @Test
    void transaction3HasCorrectIdAndNickname() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction3();
        assertEquals("0.0.11337-1568411747-660028000", tx.transactionId());
        assertEquals("Transaction 3", tx.nickname());
    }

    @Test
    void transaction3HasCorrectTransfers() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction3();
        // Transaction 3 has node account 0.0.12 instead of 0.0.3
        Map<Long, Long> expected = Map.of(
                12L, 101788L,
                98L, 83417L,
                11337L, -185205L);
        assertTransfersMatch(expected, tx.transfers());
    }

    // ==================== toString ====================

    @Test
    void toStringContainsIdAndNickname() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction1();
        String s = tx.toString();
        assertTrue(s.contains("MirrorNodeTransaction{"), "Should start with class name");
        assertTrue(s.contains("0.0.11337-1568411616-448357000"), "Should contain transaction ID");
        assertTrue(s.contains("Transaction 1"), "Should contain nickname");
    }

    @Test
    void toStringContainsTransferDetails() {
        MirrorNodeTransaction tx = MirrorNodeTransaction.getTransaction1();
        String s = tx.toString();
        assertTrue(s.contains("Account ID:"), "Should contain account ID labels");
        assertTrue(s.contains("Balance:"), "Should contain balance labels");
    }

    // ==================== transfers list properties ====================

    @Test
    void allTransactionsHaveNonEmptyTransfers() {
        assertFalse(MirrorNodeTransaction.getTransaction1().transfers().isEmpty());
        assertFalse(MirrorNodeTransaction.getTransaction2().transfers().isEmpty());
        assertFalse(MirrorNodeTransaction.getTransaction3().transfers().isEmpty());
    }

    @Test
    void transfersSumToZero() {
        // In a valid Hedera transaction, all transfers must net to zero
        for (MirrorNodeTransaction tx : List.of(
                MirrorNodeTransaction.getTransaction1(),
                MirrorNodeTransaction.getTransaction2(),
                MirrorNodeTransaction.getTransaction3())) {
            long sum = tx.transfers().stream().mapToLong(arr -> arr[1]).sum();
            assertEquals(0L, sum,
                    "Transfers for " + tx.nickname() + " should sum to zero but was " + sum);
        }
    }

    // ==================== helper ====================

    /**
     * Asserts that the actual transfers list matches the expected account-to-amount map exactly.
     */
    private static void assertTransfersMatch(Map<Long, Long> expected, List<long[]> actual) {
        assertNotNull(actual, "Transfers list should not be null");
        assertEquals(expected.size(), actual.size(),
                "Number of distinct accounts should match");
        Map<Long, Long> actualMap = new HashMap<>();
        for (long[] entry : actual) {
            assertEquals(2, entry.length, "Each transfer should have exactly 2 elements");
            actualMap.put(entry[0], entry[1]);
        }
        for (Map.Entry<Long, Long> e : expected.entrySet()) {
            assertTrue(actualMap.containsKey(e.getKey()),
                    "Missing account " + e.getKey());
            assertEquals(e.getValue(), actualMap.get(e.getKey()),
                    "Amount mismatch for account " + e.getKey());
        }
    }
}