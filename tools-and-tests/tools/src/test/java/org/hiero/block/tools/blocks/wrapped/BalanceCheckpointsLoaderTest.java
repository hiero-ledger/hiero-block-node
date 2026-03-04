// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BalanceCheckpointsLoader}.
 *
 * <p>Constructs small {@link AllAccountBalances} protobuf objects with PBJ,
 * gzips the serialized bytes, and feeds them to
 * {@link BalanceCheckpointsLoader#loadFromGzippedStream} to verify loading
 * and query behaviour without requiring external resource files.
 */
class BalanceCheckpointsLoaderTest {

    @Nested
    @DisplayName("Loading from gzipped stream")
    class LoadFromGzippedStreamTests {

        @Test
        @DisplayName("Loads HBAR balances correctly")
        void loadsHbarBalances() throws IOException {
            AllAccountBalances balances = AllAccountBalances.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(1_000L).build())
                    .allAccounts(List.of(
                            singleAccount(100L, 1_000_000L, List.of()), singleAccount(200L, 2_000_000L, List.of())))
                    .build();

            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
            loader.loadFromGzippedStream(gzipStream(balances), 10L);

            assertEquals(1, loader.getCheckpointCount());
            assertTrue(loader.hasCheckpoint(10L));
            Map<Long, Long> hbar = loader.getBalances(10L);
            assertNotNull(hbar);
            assertEquals(1_000_000L, hbar.get(100L));
            assertEquals(2_000_000L, hbar.get(200L));
        }

        @Test
        @DisplayName("Loads token balances correctly")
        void loadsTokenBalances() throws IOException {
            AllAccountBalances balances = AllAccountBalances.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(2_000L).build())
                    .allAccounts(List.of(singleAccount(
                            100L, 5_000L, List.of(tokenBalance(500L, 1_000L), tokenBalance(600L, 2_000L)))))
                    .build();

            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
            loader.loadFromGzippedStream(gzipStream(balances), 20L);

            assertTrue(loader.hasTokenBalances(20L));
            Map<Long, Map<Long, Long>> tokens = loader.getTokenBalances(20L);
            assertNotNull(tokens);
            Map<Long, Long> account100Tokens = tokens.get(100L);
            assertNotNull(account100Tokens);
            assertEquals(1_000L, account100Tokens.get(500L));
            assertEquals(2_000L, account100Tokens.get(600L));
        }

        @Test
        @DisplayName("No token balances when accounts have no tokens")
        void noTokenBalancesWhenEmpty() throws IOException {
            AllAccountBalances balances = AllAccountBalances.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(3_000L).build())
                    .allAccounts(List.of(singleAccount(100L, 5_000L, List.of())))
                    .build();

            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
            loader.loadFromGzippedStream(gzipStream(balances), 30L);

            assertFalse(loader.hasTokenBalances(30L));
            assertNull(loader.getTokenBalances(30L));
        }

        @Test
        @DisplayName("Multiple checkpoints can be loaded sequentially")
        void multipleCheckpoints() throws IOException {
            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();

            loader.loadFromGzippedStream(gzipStream(buildBalances(100L, 1_000L)), 10L);
            loader.loadFromGzippedStream(gzipStream(buildBalances(200L, 2_000L)), 20L);
            loader.loadFromGzippedStream(gzipStream(buildBalances(300L, 3_000L)), 30L);

            assertEquals(3, loader.getCheckpointCount());
            assertTrue(loader.hasCheckpoint(10L));
            assertTrue(loader.hasCheckpoint(20L));
            assertTrue(loader.hasCheckpoint(30L));
            assertFalse(loader.hasCheckpoint(15L));
        }
    }

    @Nested
    @DisplayName("Query methods")
    class QueryMethodTests {

        @Test
        @DisplayName("getNearestCheckpointAtOrBefore returns exact match")
        void nearestCheckpointExactMatch() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L, 20L, 30L);

            assertEquals(20L, loader.getNearestCheckpointAtOrBefore(20L));
        }

        @Test
        @DisplayName("getNearestCheckpointAtOrBefore returns floor when no exact match")
        void nearestCheckpointFloor() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L, 20L, 30L);

            assertEquals(20L, loader.getNearestCheckpointAtOrBefore(25L));
        }

        @Test
        @DisplayName("getNearestCheckpointAtOrBefore returns null when before all checkpoints")
        void nearestCheckpointBeforeAll() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L, 20L, 30L);

            assertNull(loader.getNearestCheckpointAtOrBefore(5L));
        }

        @Test
        @DisplayName("getNearestCheckpointAtOrBefore returns last when after all checkpoints")
        void nearestCheckpointAfterAll() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L, 20L, 30L);

            assertEquals(30L, loader.getNearestCheckpointAtOrBefore(100L));
        }

        @Test
        @DisplayName("getBalances returns null for unknown block")
        void getBalancesUnknownBlock() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L);

            assertNull(loader.getBalances(999L));
        }

        @Test
        @DisplayName("getBalancesAtOrBefore returns balances from nearest checkpoint")
        void getBalancesAtOrBefore() throws IOException {
            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
            loader.loadFromGzippedStream(gzipStream(buildBalances(100L, 1_000L)), 10L);
            loader.loadFromGzippedStream(gzipStream(buildBalances(200L, 2_000L)), 20L);

            Map<Long, Long> balances = loader.getBalancesAtOrBefore(15L);
            assertNotNull(balances);
            // Should return block 10's balances
            assertEquals(1_000L, balances.get(100L));
        }

        @Test
        @DisplayName("getFirstBlockNumber and getLastBlockNumber")
        void firstAndLastBlockNumber() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L, 20L, 30L);

            assertEquals(10L, loader.getFirstBlockNumber());
            assertEquals(30L, loader.getLastBlockNumber());
        }

        @Test
        @DisplayName("getFirstBlockNumber returns null when empty")
        void firstBlockNumberEmpty() {
            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
            assertNull(loader.getFirstBlockNumber());
        }

        @Test
        @DisplayName("getLastBlockNumber returns null when empty")
        void lastBlockNumberEmpty() {
            BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
            assertNull(loader.getLastBlockNumber());
        }

        @Test
        @DisplayName("getAllCheckpoints returns all loaded checkpoints")
        void getAllCheckpoints() throws IOException {
            BalanceCheckpointsLoader loader = loaderWithCheckpoints(10L, 20L, 30L);

            assertEquals(3, loader.getAllCheckpoints().size());
            assertTrue(loader.getAllCheckpoints().containsKey(10L));
            assertTrue(loader.getAllCheckpoints().containsKey(20L));
            assertTrue(loader.getAllCheckpoints().containsKey(30L));
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

    /** Build a simple AllAccountBalances with one account. */
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

    /** Create a loader pre-populated with checkpoints at the given block numbers. */
    private static BalanceCheckpointsLoader loaderWithCheckpoints(long... blockNumbers) throws IOException {
        BalanceCheckpointsLoader loader = new BalanceCheckpointsLoader();
        for (long blockNumber : blockNumbers) {
            loader.loadFromGzippedStream(gzipStream(buildBalances(blockNumber * 10, blockNumber * 100)), blockNumber);
        }
        return loader;
    }
}
