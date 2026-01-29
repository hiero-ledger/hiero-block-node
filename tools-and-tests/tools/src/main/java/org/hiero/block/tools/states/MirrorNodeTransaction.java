package org.hiero.block.tools.states;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This class represents a transaction on the Hedera mirror node.
 * It fetches transaction details from the mirror node API and provides methods to access the transaction ID and transfers.
 */
public class MirrorNodeTransaction {
    /**
     * Transfers for transaction 1 in the hedera history, in block 0.
     * 0.0.11337@1568411616.448357000
     * https://hashscan.io/mainnet/transaction/1568411631.396440000
     */
    public static final MirrorNodeTransaction TRANSACTION_1 = new MirrorNodeTransaction(
            "0.0.11337-1568411616-448357000", "Transaction 1");

    /**
     * Transfers for transaction 2 in the hedera history, in block 1.
     * 0.0.11337-1568411656-265684000
     * https://hashscan.io/mainnet/transaction/1568411670.872035001
     */
    public static final MirrorNodeTransaction TRANSACTION_2 = new MirrorNodeTransaction(
            "0.0.11337-1568411656-265684000", "Transaction 2");

    /**
     * Transfers for transaction 3 in the hedera history, from block 3.
     * 0.0.11337@1568411747.660028000
     * https://hashscan.io/mainnet/transaction/1568411762.486929000
     */
    public static final MirrorNodeTransaction TRANSACTION_3 = new MirrorNodeTransaction(
            "0.0.11337-1568411747-660028000", "Transaction 3");





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
     * @param transactionId the ID of the transaction
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
     * @param transactionId the ID of the transaction to download
     * @return a list of transfers, where each transfer is represented as an array of [accountId, amount]
     */
    private List<long[]> downloadTransactionDetails(String transactionId) {
        List<long[]> transfers = new ArrayList<>();
        String url = "https://mainnet-public.mirrornode.hedera.com/api/v1/transactions/" + transactionId;
        JSONObject json = null;
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            json = new JSONObject(response.body());
            JSONArray transactions = json.getJSONArray("transactions");
            if (transactions.length() == 0) return transfers;
            JSONObject tx = transactions.getJSONObject(0);
            JSONArray jsonTransfers = tx.getJSONArray("transfers");

            // Sum amounts per account
            Map<Long, Long> accountSums = new HashMap<>();
            for (int i = 0; i < jsonTransfers.length(); i++) {
                JSONObject t = jsonTransfers.getJSONObject(i);
                String accountStr = t.getString("account");
                long accountId = Long.parseLong(accountStr.substring(accountStr.lastIndexOf('.') + 1));
                long amount = t.getLong("amount");
                accountSums.put(accountId, accountSums.getOrDefault(accountId, 0L) + amount);
            }
            for (Map.Entry<Long, Long> entry : accountSums.entrySet()) {
                transfers.add(new long[]{entry.getKey(), entry.getValue()});
            }
            return transfers;
        } catch (Exception e) {
            System.err.println("Failed to download transaction details for " + transactionId + ": " + e.getMessage());
            // print url
            System.err.println("URL: " + url);
            // print json
            if (json != null) {
                System.err.println("JSON response: " + json.toString(2));
            }
            e.printStackTrace();
            return null;
        }
    }
    // https://mainnet-public.mirrornode.hedera.com/api/v1/transactions/0.0.11337-1568411747-660028000

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
          .append(" transactionId=").append(transactionId).append(",")
          .append(" nickname=").append(nickname).append(",")
          .append(" transfers=\n");
        transfers.forEach(accounts -> {
            long accountId = accounts[0];
            long balance = accounts[1];
            sb.append("    Account ID: %8d, Balance: %,16d\n".formatted(accountId, balance));
        });
        sb.append('}');
        return sb.toString();
    }

    /** main for testing the MirrorNodeTransaction class. */
    public static void main(String[] args) {
        String transactionId = "0.0.11337-1568411747-660028000"; // Example transaction ID
        MirrorNodeTransaction transaction = new MirrorNodeTransaction(transactionId, "Example Transaction");
        System.out.println(transaction);
    }
}
