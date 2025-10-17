package org.hiero.block.tools.utils.mirrornode;

import static org.hiero.block.tools.utils.mirrornode.MirrorNodeUtils.MAINNET_MIRROR_NODE_API_URL;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Utility class to fetch the address book from the mirror node.
 */
public class MirrorNodeAddressBook {

    /**
     * Get the latest address book from the mirror node.
     *
     * @return the latest address book
     */
    public static NodeAddressBook getLatestAddressBook() {
        try {
            return loadJsonAddressBook(
                URI.create(MAINNET_MIRROR_NODE_API_URL+"network/nodes?limit=40&order=asc").toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load an address book from a JSON URL.
     *
     * @param url the URL to load the address book from
     * @return the loaded address book, or an empty address book if there was an error
     */
    public static NodeAddressBook loadJsonAddressBook(URL url) {
        try {
            final var in = url.openStream();
            if (in == null) {
                return NodeAddressBook.DEFAULT; // Not available
            }
            try (in) {
                final Gson gson = new Gson();
                final AddressBookJson json = gson.fromJson(new java.io.InputStreamReader(in, StandardCharsets.UTF_8), AddressBookJson.class);
                final List<NodeAddress> nodes = new ArrayList<>();
                if (json != null && json.nodes != null) {
                    for (final AddressBookJson.Node n : json.nodes) {
                        final long accountNum = parseAccountNum(n.nodeAccountId);
                        final String memoStr = "0.0." + accountNum;
                        final String rsaHex = strip0x(n.publicKey);
                        final byte[] certHash = parseHexOrEmpty(n.nodeCertHash);

                        final AccountID accountID = AccountID.newBuilder()
                            .shardNum(0)
                            .realmNum(0)
                            .accountNum(accountNum)
                            .build();

                        final NodeAddress node = NodeAddress.newBuilder()
                            .memo(Bytes.wrap(memoStr.getBytes(StandardCharsets.UTF_8)))
                            .rsaPubKey(rsaHex)
                            .nodeId(n.nodeId)
                            .nodeAccountId(accountID)
                            .nodeCertHash(Bytes.wrap(certHash))
                            .build();
                        nodes.add(node);
                    }
                }
                return new NodeAddressBook(nodes);
            }
        } catch (Exception e) {
            // If anything goes wrong, return an empty book to avoid breaking callers.
            return NodeAddressBook.DEFAULT;
        }
    }

    /**
     * Strip "0x" or "0X" prefix from a hex string if present.
     *
     * @param s the string to strip
     * @return the stripped string, or empty string if input is null
     */
    private static String strip0x(String s) {
        if (s == null) return "";
        return s.startsWith("0x") || s.startsWith("0X") ? s.substring(2) : s;
    }

    /**
     * Parse the account number from a string in the format "shard.realm.num".
     *
     * @param account the account string
     * @return the account number, or 0 if input is null or blank
     */
    private static long parseAccountNum(String account) {
        if (account == null || account.isBlank()) return 0L;
        // expects format like "0.0.23"
        final String[] parts = account.split("\\.");
        return Long.parseLong(parts[parts.length - 1]);
    }

    /**
     * Parse a hex string into a byte array, or return an empty array if input is null, blank, or invalid.
     *
     * @param hex the hex string to parse
     * @return the parsed byte array, or empty array if input is null, blank, or invalid
     */
    private static byte[] parseHexOrEmpty(String hex) {
        if (hex == null || hex.isBlank()) return new byte[0];
        final String s = strip0x(hex);
        try {
            return HexFormat.of().parseHex(s);
        } catch (Exception e) {
            return new byte[0];
        }
    }

    /**
     * Minimal JSON mapping classes
     * Only includes fields we care about.
     * Uses Gson annotations to map JSON fields to Java fields.
     */
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final class AddressBookJson {
        List<Node> nodes;
        static final class Node {
            @SerializedName("node_id") long nodeId;
            @SerializedName("node_account_id") String nodeAccountId;
            @SerializedName("public_key") String publicKey;
            @SerializedName("node_cert_hash") String nodeCertHash;
        }
    }
}
