// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode.model;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class represents a transaction on the Hedera mirror node.
 * It fetches transaction details from the mirror node API and provides methods to access the transaction ID and transfers.
 */
@SuppressWarnings("unused")
public final class MirrorNodeTransaction {
    /**
     * Transfers for transaction 1 in the hedera history, in block 0. 0.0.11337@1568411616.448357000
     * @see <a href="https://hashscan.io/mainnet/transaction/1568411631.396440000">T1 Explorer</a>
     * @see <a href="https://mainnet-public.mirrornode.hedera.com/api/v1/transactions/0.0.11337-1568411616-448357000">T1 Mirror Node</a>
     */
    public static MirrorNodeTransaction getTransaction1() {
        return new MirrorNodeTransaction("0.0.11337-1568411616-448357000", "Transaction 1");
    }

    /**
     * Transfers for transaction 2 in the hedera history, in block 1. 0.0.11337-1568411656-265684000
     * @see <a href="https://hashscan.io/mainnet/transaction/1568411670.872035001">T2 Explorer</a>
     * @see <a href="https://mainnet-public.mirrornode.hedera.com/api/v1/transactions/0.0.11337-1568411656-265684000">T2 Mirror Node</a>
     */
    public static MirrorNodeTransaction getTransaction2() {
        return new MirrorNodeTransaction("0.0.11337-1568411656-265684000", "Transaction 2");
    }

    /**
     * Transfers for transaction 3 in the hedera history, from block 3. 0.0.11337@1568411747.660028000
     * @see <a href="https://hashscan.io/mainnet/transaction/1568411762.486929000">T3 Explorer</a>
     * @see <a href="https://mainnet-public.mirrornode.hedera.com/api/v1/transactions/0.0.11337-1568411747-660028000">T3 Mirror Node</a>
     */
    public static MirrorNodeTransaction getTransaction3() {
        return new MirrorNodeTransaction("0.0.11337-1568411747-660028000", "Transaction 3");
    }

    /** The transaction ID of the Hedera transaction. */
    private final String transactionId;
    /** A nickname for the transaction, used for easier identification. */
    private final String nickname;
    /** A list of transfers associated with the transaction, where each transfer is represented as an array of [accountId, amount]. */
    private final List<long[]> transfers;

    /**
     * Constructs a MirrorNodeTransaction with the specified transaction ID and nickname.
     * It downloads and parses the transaction details from the Hedera mirror node API.
     *
     * @param transactionId the ID of the transaction in mirror node format (e.g., "0.0.11337-1568411616-448357000")
     * @param nickname a nickname for the transaction
     */
    public MirrorNodeTransaction(String transactionId, String nickname) {
        this.transactionId = transactionId;
        this.nickname = nickname;
        // download and parse the transaction details from the mirror node
        this.transfers = downloadTransactionDetails(transactionId);
    }

    /**
     * Downloads the transaction details from the Hedera mirror node API.
     *
     * @param transactionId the ID of the transaction to download in mirror node format (e.g., "0.0.11337-1568411616-448357000")
     * @return a list of transfers, where each transfer is represented as an array of [accountId, amount]
     */
    private List<long[]> downloadTransactionDetails(String transactionId) {
        List<long[]> transfers = new ArrayList<>();
        String url = "https://mainnet-public.mirrornode.hedera.com/api/v1/transactions/" + transactionId;
        String responseBody = null;
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            responseBody = response.body();
            Gson gson = new Gson();
            TransactionsResponse parsed = gson.fromJson(responseBody, TransactionsResponse.class);
            if (parsed == null || parsed.transactions == null || parsed.transactions.isEmpty()) return transfers;
            TransactionJson tx = parsed.transactions.getFirst();
            if (tx.transfers == null) return transfers;

            // Sum amounts per account
            Map<Long, Long> accountSums = new HashMap<>();
            for (TransferJson t : tx.transfers) {
                long accountId = Long.parseLong(t.account.substring(t.account.lastIndexOf('.') + 1));
                accountSums.put(accountId, accountSums.getOrDefault(accountId, 0L) + t.amount);
            }
            for (Map.Entry<Long, Long> entry : accountSums.entrySet()) {
                transfers.add(new long[] {entry.getKey(), entry.getValue()});
            }
            return transfers;
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to download transaction details for " + transactionId + " @ " + url + " responseBody="
                            + responseBody + " : " + e.getMessage(),
                    e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the transaction id of the transaction.
     *
     * @return the transaction id
     */
    public String transactionId() {
        return transactionId;
    }

    /**
     * Returns the nickname of the transaction.
     *
     * @return the nickname
     */
    public String nickname() {
        return nickname;
    }

    /**
     * Returns the list of transfers associated with the transaction.
     * Each transfer is represented as an array of [accountId, amount].
     * Amounts are in tinybars, so they can be large numbers.
     *
     * @return the list of transfers
     */
    public List<long[]> transfers() {
        return transfers;
    }

    /**
     * Returns a string representation of the transaction, including its ID, nickname, and transfers.
     *
     * @return a string representation of the transaction
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MirrorNodeTransaction{")
                .append(" transactionId=")
                .append(transactionId)
                .append(",")
                .append(" nickname=")
                .append(nickname)
                .append(",")
                .append(" transfers=\n");
        transfers.forEach(accounts -> {
            long accountId = accounts[0];
            long balance = accounts[1];
            sb.append("    Account ID: %8d, Balance: %,16d\n".formatted(accountId, balance));
        });
        sb.append('}');
        return sb.toString();
    }

    // ======= JSON Mapping Classes ====================================================================================

    /** JSON mapping for the transactions API response. */
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final class TransactionsResponse {
        List<TransactionJson> transactions;
    }

    /** JSON mapping for a single transaction. */
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final class TransactionJson {
        List<TransferJson> transfers;
    }

    /** JSON mapping for a single transfer entry. */
    private static final class TransferJson {
        String account;
        long amount;
    }
}
