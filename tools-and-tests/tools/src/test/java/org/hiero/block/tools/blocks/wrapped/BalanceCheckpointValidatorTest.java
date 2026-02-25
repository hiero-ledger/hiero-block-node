// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.TokenID;
import com.hedera.hapi.streams.AllAccountBalances;
import com.hedera.hapi.streams.SingleAccountBalances;
import com.hedera.hapi.streams.TokenUnitBalance;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BalanceCheckpointValidator}.
 *
 * <p>Constructs small {@link AllAccountBalances} protobuf objects with PBJ,
 * gzips the serialized bytes, and loads them via
 * {@link BalanceCheckpointValidator#loadFromGzippedStream} to exercise
 * checkpoint loading, balance validation, interval filtering, and summary
 * reporting.
 */
class BalanceCheckpointValidatorTest {

    @Nested
    @DisplayName("Loading checkpoints")
    class LoadingTests {

        @Test
        @DisplayName("loadFromGzippedStream loads balances and increments checkpoint count")
        void loadFromGzippedStreamLoadsBalances() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);

            assertEquals(1, validator.getCheckpointCount());
        }

        @Test
        @DisplayName("Multiple gzipped streams can be loaded")
        void multipleGzippedStreamsLoaded() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);
            validator.loadFromGzippedStream(gzipStream(buildBalances(200L, 6_000L)), 20L);

            assertEquals(2, validator.getCheckpointCount());
        }
    }

    @Nested
    @DisplayName("checkBlock validation")
    class CheckBlockTests {

        @Test
        @DisplayName("checkBlock passes when computed balances match expected")
        void checkBlockPassesOnMatch() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);

            Map<Long, Long> computed = new HashMap<>();
            computed.put(100L, 5_000L);

            assertDoesNotThrow(() -> validator.checkBlock(10L, computed));
            assertTrue(validator.allPassed());
        }

        @Test
        @DisplayName("checkBlock throws ValidationException on HBAR mismatch")
        void checkBlockThrowsOnHbarMismatch() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);

            Map<Long, Long> computed = new HashMap<>();
            computed.put(100L, 9_999L); // wrong balance

            ValidationException ex = assertThrows(ValidationException.class, () -> validator.checkBlock(10L, computed));
            assertTrue(ex.getMessage().contains("Balance validation failed"));
        }

        @Test
        @DisplayName("checkBlock throws ValidationException when account is missing from computed")
        void checkBlockThrowsOnMissingAccount() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);

            Map<Long, Long> computed = new HashMap<>();
            // Account 100 is missing entirely

            assertThrows(ValidationException.class, () -> validator.checkBlock(10L, computed));
        }

        @Test
        @DisplayName("checkBlock passes with token balances matching")
        void checkBlockPassesWithTokens() throws IOException {
            AllAccountBalances balances = AllAccountBalances.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(1_000L).build())
                    .allAccounts(List.of(singleAccount(100L, 5_000L, List.of(tokenBalance(500L, 1_000L)))))
                    .build();

            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(balances), 10L);

            Map<Long, Long> computedHbar = new HashMap<>();
            computedHbar.put(100L, 5_000L);

            Map<Long, Map<Long, Long>> computedTokens = new HashMap<>();
            computedTokens.put(100L, Map.of(500L, 1_000L));

            assertDoesNotThrow(() -> validator.checkBlock(10L, computedHbar, computedTokens));
            assertTrue(validator.allPassed());
        }

        @Test
        @DisplayName("checkBlock throws on token balance mismatch")
        void checkBlockThrowsOnTokenMismatch() throws IOException {
            AllAccountBalances balances = AllAccountBalances.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(1_000L).build())
                    .allAccounts(List.of(singleAccount(100L, 5_000L, List.of(tokenBalance(500L, 1_000L)))))
                    .build();

            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(balances), 10L);

            Map<Long, Long> computedHbar = new HashMap<>();
            computedHbar.put(100L, 5_000L);

            Map<Long, Map<Long, Long>> computedTokens = new HashMap<>();
            computedTokens.put(100L, Map.of(500L, 9_999L)); // wrong token balance

            assertThrows(ValidationException.class, () -> validator.checkBlock(10L, computedHbar, computedTokens));
        }

        @Test
        @DisplayName("checkBlock does nothing when no checkpoints are loaded")
        void checkBlockNoCheckpoints() {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();

            Map<Long, Long> computed = new HashMap<>();
            computed.put(100L, 5_000L);

            assertDoesNotThrow(() -> validator.checkBlock(10L, computed));
        }

        @Test
        @DisplayName("checkBlock validates multiple checkpoints as block number advances")
        void checkBlockMultipleCheckpoints() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 1_000L)), 10L);
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 2_000L)), 20L);

            Map<Long, Long> computed1 = new HashMap<>();
            computed1.put(100L, 1_000L);
            assertDoesNotThrow(() -> validator.checkBlock(10L, computed1));

            Map<Long, Long> computed2 = new HashMap<>();
            computed2.put(100L, 2_000L);
            assertDoesNotThrow(() -> validator.checkBlock(20L, computed2));

            assertTrue(validator.allPassed());
        }
    }

    @Nested
    @DisplayName("Check interval filtering")
    class CheckIntervalTests {

        @Test
        @DisplayName("setCheckIntervalDays filters checkpoints by block interval")
        void intervalFiltersCheckpoints() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            // Set interval to 1 day = 20,000 blocks
            validator.setCheckIntervalDays(1);

            // Load checkpoints close together (only 5,000 blocks apart)
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 1_000L)), 10_000L);
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 1_500L)), 15_000L);
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 2_000L)), 35_000L);

            // First and third should be included (gap of 25,000 >= 20,000)
            // Second should be filtered out (gap of 5,000 < 20,000)
            // We verify by passing matching balances for first and third,
            // but a wrong balance for the second - if the second were checked it would fail

            Map<Long, Long> computed1 = new HashMap<>();
            computed1.put(100L, 1_000L);
            assertDoesNotThrow(() -> validator.checkBlock(10_000L, computed1));

            // Block 15,000 should be skipped by interval filter, so wrong balance is fine
            Map<Long, Long> computedWrong = new HashMap<>();
            computedWrong.put(100L, 9_999L);
            assertDoesNotThrow(() -> validator.checkBlock(15_000L, computedWrong));

            Map<Long, Long> computed3 = new HashMap<>();
            computed3.put(100L, 2_000L);
            assertDoesNotThrow(() -> validator.checkBlock(35_000L, computed3));

            assertTrue(validator.allPassed());
        }

        @Test
        @DisplayName("setCheckIntervalDays(0) checks all checkpoints")
        void zeroIntervalChecksAll() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.setCheckIntervalDays(0);

            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 1_000L)), 10L);
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 2_000L)), 20L);

            Map<Long, Long> computed1 = new HashMap<>();
            computed1.put(100L, 1_000L);
            assertDoesNotThrow(() -> validator.checkBlock(10L, computed1));

            Map<Long, Long> computed2 = new HashMap<>();
            computed2.put(100L, 2_000L);
            assertDoesNotThrow(() -> validator.checkBlock(20L, computed2));

            assertTrue(validator.allPassed());
        }
    }

    @Nested
    @DisplayName("hasCheckpointsInRange")
    class HasCheckpointsInRangeTests {

        @Test
        @DisplayName("Returns true when checkpoint exists in range")
        void checkpointInRange() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 50L);

            assertTrue(validator.hasCheckpointsInRange(40L, 60L));
        }

        @Test
        @DisplayName("Returns true when checkpoint is at range boundary")
        void checkpointAtBoundary() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 50L);

            assertTrue(validator.hasCheckpointsInRange(50L, 100L));
            assertTrue(validator.hasCheckpointsInRange(1L, 50L));
        }

        @Test
        @DisplayName("Returns false when no checkpoint in range")
        void noCheckpointInRange() throws IOException {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 50L);

            assertFalse(validator.hasCheckpointsInRange(51L, 100L));
            assertFalse(validator.hasCheckpointsInRange(1L, 49L));
        }

        @Test
        @DisplayName("Returns false when no checkpoints loaded")
        void noCheckpointsLoaded() {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();

            assertFalse(validator.hasCheckpointsInRange(1L, 100L));
        }
    }

    @Nested
    @DisplayName("Summary and allPassed")
    class SummaryTests {

        @Test
        @DisplayName("allPassed returns true when all checkpoints pass")
        void allPassedTrue() throws Exception {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);

            Map<Long, Long> computed = new HashMap<>();
            computed.put(100L, 5_000L);
            validator.checkBlock(10L, computed);

            assertTrue(validator.allPassed());
        }

        @Test
        @DisplayName("allPassed returns true when no validations performed")
        void allPassedNoValidations() {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            assertTrue(validator.allPassed());
        }

        @Test
        @DisplayName("printSummary does not throw when no validations performed")
        void printSummaryNoValidations() {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            assertDoesNotThrow(validator::printSummary);
        }

        @Test
        @DisplayName("printSummary does not throw after successful validation")
        void printSummaryAfterSuccess() throws Exception {
            BalanceCheckpointValidator validator = new BalanceCheckpointValidator();
            validator.loadFromGzippedStream(gzipStream(buildBalances(100L, 5_000L)), 10L);

            Map<Long, Long> computed = new HashMap<>();
            computed.put(100L, 5_000L);
            validator.checkBlock(10L, computed);

            assertDoesNotThrow(validator::printSummary);
        }
    }

    // ===== Helpers =====

    /** Create a SingleAccountBalances with the given account number, HBAR balance, and token balances. */
    private static SingleAccountBalances singleAccount(
            long accountNum, long hbarBalance, List<TokenUnitBalance> tokenBalances) {
        return SingleAccountBalances.newBuilder()
                .accountID(AccountID.newBuilder()
                        .shardNum(0)
                        .realmNum(0)
                        .accountNum(accountNum)
                        .build())
                .hbarBalance(hbarBalance)
                .tokenUnitBalances(tokenBalances)
                .build();
    }

    /** Create a TokenUnitBalance for the given token number and balance. */
    private static TokenUnitBalance tokenBalance(long tokenNum, long balance) {
        return TokenUnitBalance.newBuilder()
                .tokenId(TokenID.newBuilder()
                        .shardNum(0)
                        .realmNum(0)
                        .tokenNum(tokenNum)
                        .build())
                .balance(balance)
                .build();
    }

    /** Build a simple AllAccountBalances with one account (HBAR only). */
    private static AllAccountBalances buildBalances(long accountNum, long hbarBalance) {
        return AllAccountBalances.newBuilder()
                .consensusTimestamp(Timestamp.newBuilder().seconds(1_000L).build())
                .allAccounts(List.of(singleAccount(accountNum, hbarBalance, List.of())))
                .build();
    }

    /** Serialize an AllAccountBalances to gzipped bytes and return as an InputStream. */
    private static ByteArrayInputStream gzipStream(AllAccountBalances balances) throws IOException {
        byte[] pbBytes = AllAccountBalances.PROTOBUF.toBytes(balances).toByteArray();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzOut = new GZIPOutputStream(baos)) {
            gzOut.write(pbBytes);
        }
        return new ByteArrayInputStream(baos.toByteArray());
    }
}
