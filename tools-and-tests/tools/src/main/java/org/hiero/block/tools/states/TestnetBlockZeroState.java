// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import com.google.gson.Gson;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.AccountID.AccountOneOfType;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.token.Account;
import com.hedera.pbj.runtime.OneOf;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.tools.config.NetworkConfig;

/**
 * Provides the genesis state for testnet block zero by fetching account balances from the
 * testnet mirror node API near genesis time.
 *
 * <p>Testnet was reset in February 2024. Genesis date: 2024-02-01, first block timestamp:
 * 2024-02-01T18:35:20.644859297Z, nodes: 0.0.3 through 0.0.9 (7 nodes).
 *
 * <p>Unlike mainnet (which requires a saved state snapshot and transaction reversal), testnet
 * genesis state can be fetched from the mirror node API since the network was freshly reset at
 * that point. The exact genesis timestamp returns empty results because mirror node balance
 * snapshots are periodic, so we use the earliest available snapshot (~72 minutes after genesis).
 * This snapshot captures the true genesis state: the treasury (0.0.2) holds all HBAR, and
 * system accounts (0.0.1 through ~0.0.750+) exist with 0 balance before any meaningful
 * transactions have occurred.
 */
public final class TestnetBlockZeroState {

    /** Genesis timestamp for testnet, used for the consensus timestamp in state change block items. */
    private static final String GENESIS_TIMESTAMP = "2024-02-01T18:35:20.644859297Z";

    /**
     * Earliest available balance snapshot timestamp on the testnet mirror node.
     * The exact genesis timestamp returns empty results because balance snapshots are periodic.
     * This timestamp (~72 min after genesis) captures the genesis state before any meaningful
     * transactions have occurred.
     */
    private static final String EARLIEST_SNAPSHOT_TIMESTAMP = "1706815000.000000000";

    /** Cached genesis state changes */
    private static List<BlockItem> cachedStateChanges;

    private TestnetBlockZeroState() {
        // utility class
    }

    /**
     * Get the block items representing the STATE_CHANGES for the initial state at the beginning
     * of testnet block zero.
     *
     * <p>Fetches account balances from the testnet mirror node API at genesis time and converts
     * them to BlockItem STATE_CHANGES in the same format as mainnet genesis state.
     *
     * @return list of block items representing the state changes at the start of block zero
     */
    public static synchronized List<BlockItem> loadStartBlockZeroStateChanges() {
        if (cachedStateChanges == null) {
            cachedStateChanges = fetchAndConvertGenesisState();
        }
        return cachedStateChanges;
    }

    /**
     * Fetches account balances from the mirror node API and converts to STATE_CHANGES.
     */
    private static List<BlockItem> fetchAndConvertGenesisState() {
        System.out.println("Fetching testnet genesis account balances from mirror node...");

        final Instant genesisInstant = Instant.parse(GENESIS_TIMESTAMP);
        final Timestamp consensusTimestamp = new Timestamp(genesisInstant.getEpochSecond(), genesisInstant.getNano());

        // Fetch all account balances at genesis time
        List<AccountBalance> balances = fetchAllBalances();
        System.out.println("Fetched " + balances.size() + " account balances at testnet genesis");

        // Convert to STATE_CHANGES
        List<StateChange> accountStateChanges = new ArrayList<>();
        for (AccountBalance ab : balances) {
            long accountNum = parseAccountNum(ab.account);
            AccountID accountId = new AccountID(0, 0, new OneOf<>(AccountOneOfType.ACCOUNT_NUM, accountNum));

            Account account = Account.newBuilder()
                    .accountId(accountId)
                    .tinybarBalance(ab.balance)
                    .build();

            accountStateChanges.add(StateChange.newBuilder()
                    .stateId(StateIdentifier.STATE_ID_ACCOUNTS.protoOrdinal())
                    .mapUpdate(MapUpdateChange.newBuilder()
                            .key(MapChangeKey.newBuilder().accountIdKey(accountId))
                            .value(MapChangeValue.newBuilder().accountValue(account)))
                    .build());
        }

        List<BlockItem> blockItems = new ArrayList<>();
        blockItems.add(new BlockItem(
                new OneOf<>(ItemOneOfType.STATE_CHANGES, new StateChanges(consensusTimestamp, accountStateChanges))));

        return blockItems;
    }

    /**
     * Fetches all account balances from the mirror node API at genesis time, handling pagination.
     */
    private static List<AccountBalance> fetchAllBalances() {
        List<AccountBalance> allBalances = new ArrayList<>();
        String baseUrl = NetworkConfig.testnet().mirrorNodeApiUrl();
        // Extract host portion (e.g. "https://testnet.mirrornode.hedera.com") for building
        // absolute URLs from relative "next" links returned by the API.
        String baseHost = baseUrl.substring(0, baseUrl.indexOf("/api/v1/"));
        String nextUrl = baseUrl + "balances?timestamp=" + EARLIEST_SNAPSHOT_TIMESTAMP + "&limit=100&order=asc";

        while (nextUrl != null) {
            BalancesResponse response = fetchBalancesPage(nextUrl);
            if (response.balances != null) {
                allBalances.addAll(response.balances);
            }

            // Handle pagination
            if (response.links != null && response.links.next != null) {
                String next = response.links.next;
                // The next link is a relative path starting with /api/v1/
                nextUrl = next.startsWith("/") ? baseHost + next : baseUrl + next;
            } else {
                nextUrl = null;
            }
        }

        return allBalances;
    }

    /**
     * Fetches a single page of balances from the mirror node API.
     */
    private static BalancesResponse fetchBalancesPage(String url) {
        try {
            HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
            conn.setConnectTimeout(30_000);
            conn.setReadTimeout(30_000);

            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("HTTP " + responseCode + " fetching balances from: " + url);
            }

            try (var reader = new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8)) {
                return new Gson().fromJson(reader, BalancesResponse.class);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to fetch balances from: " + url, e);
        }
    }

    /**
     * Parse account number from "0.0.N" format.
     */
    private static long parseAccountNum(String account) {
        String[] parts = account.split("\\.");
        return Long.parseLong(parts[parts.length - 1]);
    }

    // JSON mapping classes for mirror node balances API

    @SuppressWarnings("unused")
    private static final class BalancesResponse {
        List<AccountBalance> balances;
        Links links;
        String timestamp;
    }

    @SuppressWarnings("unused")
    private static final class AccountBalance {
        String account;
        long balance;
    }

    @SuppressWarnings("unused")
    private static final class Links {
        String next;
    }
}
