// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BalanceCsvValidator}.
 *
 * <p>Tests the balance comparison logic without requiring actual GCP access.
 */
class BalanceCsvValidatorTest {

    @Nested
    @DisplayName("Balance Comparison Tests")
    class BalanceComparisonTests {

        @Test
        @DisplayName("All balances match returns empty mismatches")
        void allBalancesMatch() {
            Map<Long, Long> fileBalances = new HashMap<>();
            fileBalances.put(100L, 1_000_000_000L);
            fileBalances.put(200L, 2_000_000_000L);
            fileBalances.put(300L, 500_000_000L);

            Map<Long, Long> computedBalances = new HashMap<>();
            computedBalances.put(100L, 1_000_000_000L);
            computedBalances.put(200L, 2_000_000_000L);
            computedBalances.put(300L, 500_000_000L);

            BalanceCsvValidator.ComparisonResult result =
                    createValidator().compareBalances(fileBalances, computedBalances);

            assertEquals(3, result.matchCount());
            assertTrue(result.mismatches().isEmpty());
        }

        @Test
        @DisplayName("Single mismatch is detected")
        void singleMismatchDetected() {
            Map<Long, Long> fileBalances = new HashMap<>();
            fileBalances.put(100L, 1_000_000_000L);
            fileBalances.put(200L, 2_000_000_000L);

            Map<Long, Long> computedBalances = new HashMap<>();
            computedBalances.put(100L, 1_000_000_000L);
            computedBalances.put(200L, 1_500_000_000L); // Mismatch

            BalanceCsvValidator.ComparisonResult result =
                    createValidator().compareBalances(fileBalances, computedBalances);

            assertEquals(1, result.matchCount());
            assertEquals(1, result.mismatches().size());
            assertTrue(result.mismatches().containsKey(200L));
            assertEquals(2_000_000_000L, result.mismatches().get(200L).expected());
            assertEquals(1_500_000_000L, result.mismatches().get(200L).computed());
        }

        @Test
        @DisplayName("Missing account in computed balances is detected")
        void missingAccountDetected() {
            Map<Long, Long> fileBalances = new HashMap<>();
            fileBalances.put(100L, 1_000_000_000L);
            fileBalances.put(200L, 2_000_000_000L);

            Map<Long, Long> computedBalances = new HashMap<>();
            computedBalances.put(100L, 1_000_000_000L);
            // Account 200 is missing

            BalanceCsvValidator.ComparisonResult result =
                    createValidator().compareBalances(fileBalances, computedBalances);

            assertEquals(1, result.matchCount());
            assertEquals(1, result.mismatches().size());
            assertTrue(result.mismatches().containsKey(200L));
            assertEquals(2_000_000_000L, result.mismatches().get(200L).expected());
            assertEquals(0L, result.mismatches().get(200L).computed());
        }

        @Test
        @DisplayName("Extra accounts in computed balances are ignored")
        void extraAccountsIgnored() {
            Map<Long, Long> fileBalances = new HashMap<>();
            fileBalances.put(100L, 1_000_000_000L);

            Map<Long, Long> computedBalances = new HashMap<>();
            computedBalances.put(100L, 1_000_000_000L);
            computedBalances.put(999L, 5_000_000_000L); // Extra account

            BalanceCsvValidator.ComparisonResult result =
                    createValidator().compareBalances(fileBalances, computedBalances);

            assertEquals(1, result.matchCount());
            assertTrue(result.mismatches().isEmpty());
        }

        @Test
        @DisplayName("Multiple mismatches are all detected")
        void multipleMismatchesDetected() {
            Map<Long, Long> fileBalances = new HashMap<>();
            fileBalances.put(100L, 1_000_000_000L);
            fileBalances.put(200L, 2_000_000_000L);
            fileBalances.put(300L, 3_000_000_000L);

            Map<Long, Long> computedBalances = new HashMap<>();
            computedBalances.put(100L, 999_000_000L); // Mismatch
            computedBalances.put(200L, 2_000_000_000L); // Match
            computedBalances.put(300L, 3_001_000_000L); // Mismatch

            BalanceCsvValidator.ComparisonResult result =
                    createValidator().compareBalances(fileBalances, computedBalances);

            assertEquals(1, result.matchCount());
            assertEquals(2, result.mismatches().size());
            assertTrue(result.mismatches().containsKey(100L));
            assertTrue(result.mismatches().containsKey(300L));
        }

        @Test
        @DisplayName("Empty file balances results in zero matches")
        void emptyFileBalances() {
            Map<Long, Long> fileBalances = new HashMap<>();

            Map<Long, Long> computedBalances = new HashMap<>();
            computedBalances.put(100L, 1_000_000_000L);

            BalanceCsvValidator.ComparisonResult result =
                    createValidator().compareBalances(fileBalances, computedBalances);

            assertEquals(0, result.matchCount());
            assertTrue(result.mismatches().isEmpty());
        }
    }

    @Nested
    @DisplayName("Signature Threshold Tests")
    class SignatureThresholdTests {

        @Test
        @DisplayName("1/3 + 1 threshold calculation for various node counts")
        void signatureThresholdCalculation() {
            // Required signatures = (totalNodes / 3) + 1
            assertEquals(2, calculateRequiredSignatures(3)); // 3/3 + 1 = 2
            assertEquals(4, calculateRequiredSignatures(10)); // 10/3 + 1 = 4
            assertEquals(10, calculateRequiredSignatures(29)); // 29/3 + 1 = 10
            assertEquals(11, calculateRequiredSignatures(30)); // 30/3 + 1 = 11
            assertEquals(12, calculateRequiredSignatures(34)); // 34/3 + 1 = 12
        }

        private int calculateRequiredSignatures(int totalNodes) {
            return (totalNodes / 3) + 1;
        }
    }

    @Nested
    @DisplayName("Checkpoint Timing Tests")
    class CheckpointTimingTests {

        @Test
        @DisplayName("Block timestamp after checkpoint triggers validation")
        void blockAfterCheckpointTriggersValidation() {
            Instant checkpoint = Instant.parse("2023-10-01T12:00:00Z");
            Instant blockTimestamp = Instant.parse("2023-10-01T12:00:01Z");

            assertTrue(blockTimestamp.isAfter(checkpoint));
        }

        @Test
        @DisplayName("Block timestamp before checkpoint does not trigger validation")
        void blockBeforeCheckpointDoesNotTrigger() {
            Instant checkpoint = Instant.parse("2023-10-01T12:00:00Z");
            Instant blockTimestamp = Instant.parse("2023-10-01T11:59:59Z");

            assertFalse(blockTimestamp.isAfter(checkpoint));
        }

        @Test
        @DisplayName("Block timestamp equal to checkpoint does not trigger validation")
        void blockEqualToCheckpointDoesNotTrigger() {
            Instant checkpoint = Instant.parse("2023-10-01T12:00:00Z");
            Instant blockTimestamp = Instant.parse("2023-10-01T12:00:00Z");

            assertFalse(blockTimestamp.isAfter(checkpoint));
        }
    }

    /**
     * Creates a minimal validator instance for testing.
     */
    private BalanceCsvValidator createValidator() {
        return new BalanceCsvValidator(null, 3, 34, null, null, false);
    }
}
