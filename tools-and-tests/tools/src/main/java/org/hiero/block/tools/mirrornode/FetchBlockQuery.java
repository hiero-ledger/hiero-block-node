// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.MAINNET_MIRROR_NODE_API_URL;

import com.google.gson.JsonObject;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Query Mirror Node and fetch block information
 */
@SuppressWarnings("unused")
public class FetchBlockQuery {

    /**
     * Get the record file name for a block number from the mirror node.
     *
     * @param blockNumber the block number
     * @return the record file name
     */
    public static String getRecordFileNameForBlock(long blockNumber) {
        final String url = MAINNET_MIRROR_NODE_API_URL + "blocks/" + blockNumber;
        final JsonObject json = MirrorNodeUtils.readUrl(url);
        return json.get("name").getAsString();
    }

    /**
     * Get the previous hash for a block number from the mirror node.
     *
     * @param blockNumber the block number
     * @return the record file name
     */
    public static Bytes getPreviousHashForBlock(long blockNumber) {
        final String url = MAINNET_MIRROR_NODE_API_URL + "blocks/" + blockNumber;
        final JsonObject json = MirrorNodeUtils.readUrl(url);
        final String hashStr = json.get("previous_hash").getAsString();
        return Bytes.wrap(HexFormat.of().parseHex(hashStr.substring(2))); // remove 0x prefix and parse
    }

    /**
     * Get the latest blocks from the mirror node and return as list of objects.
     *
     * @param limit number of blocks to retrieve
     * @param order ordering of blocks
     * @return a list of BlockInfo objects representing the latest blocks
     */
    public static List<BlockInfo> getLatestBlocks(int limit, MirrorNodeBlockQueryOrder order) {
        return getLatestBlocks(limit, order, null);
    }

    /**
     * Get the latest blocks from the mirror node and return as list of objects, optionally
     * constrained by one or more timestamp filters.
     *
     * <p>The Mirror Node API defines {@code timestamp} as an array of {@code string}, where each
     * entry is of the form {@code <op>:<seconds.nanoseconds>} such as
     * {@code gte:1700000000.000000000} or {@code lt:1700003600.000000000}. Multiple filters are applied
     * as repeated {@code timestamp=} query parameters.</p>
     *
     * @param limit number of blocks to retrieve
     * @param order ordering of blocks
     * @param timestampFilters optional list of timestamp filter expressions (e.g. {@code gte:...}, {@code lt:...});
     *                        may be {@code null} or empty to omit the parameter
     * @return a list of BlockInfo objects representing the latest blocks
     */
    public static List<BlockInfo> getLatestBlocks(
            int limit, MirrorNodeBlockQueryOrder order, List<String> timestampFilters) {
        final StringBuilder url = new StringBuilder();
        url.append(MAINNET_MIRROR_NODE_API_URL)
                .append("blocks")
                .append("?limit=")
                .append(limit)
                .append("&order=")
                .append(order.name());

        if (timestampFilters != null && !timestampFilters.isEmpty()) {
            for (String ts : timestampFilters) {
                if (ts == null || ts.isBlank()) {
                    continue;
                }
                url.append("&timestamp=").append(URLEncoder.encode(ts, StandardCharsets.UTF_8));
            }
        }

        final JsonObject json = MirrorNodeUtils.readUrl(url.toString());
        List<BlockInfo> blocks = new ArrayList<>();

        if (json.has("blocks") && json.get("blocks").isJsonArray()) {
            json.getAsJsonArray("blocks").forEach(elem -> {
                JsonObject b = elem.getAsJsonObject();
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.count = b.has("count") ? b.get("count").getAsInt() : 0;
                blockInfo.hapiVersion = (b.has("hapi_version")
                                && !b.get("hapi_version").isJsonNull())
                        ? b.get("hapi_version").getAsString()
                        : null;
                blockInfo.hash = (b.has("hash") && !b.get("hash").isJsonNull())
                        ? b.get("hash").getAsString()
                        : null;
                blockInfo.name = (b.has("name") && !b.get("name").isJsonNull())
                        ? b.get("name").getAsString()
                        : null;
                blockInfo.number = b.has("number") ? b.get("number").getAsLong() : -1;
                blockInfo.previousHash = (b.has("previous_hash")
                                && !b.get("previous_hash").isJsonNull())
                        ? b.get("previous_hash").getAsString()
                        : null;
                blockInfo.size = b.has("size") && !b.get("size").isJsonNull()
                        ? b.get("size").getAsLong()
                        : 0;
                blockInfo.gasUsed = b.has("gas_used") ? b.get("gas_used").getAsLong() : 0;
                if (b.has("timestamp") && b.get("timestamp").isJsonObject()) {
                    JsonObject tsObj = b.getAsJsonObject("timestamp");
                    blockInfo.timestampFrom = (tsObj.has("from")
                                    && !tsObj.get("from").isJsonNull())
                            ? tsObj.get("from").getAsString()
                            : null;
                    blockInfo.timestampTo = (tsObj.has("to") && !tsObj.get("to").isJsonNull())
                            ? tsObj.get("to").getAsString()
                            : null;
                }
                blocks.add(blockInfo);
            });
        }

        return blocks;
    }

    /**
     * Test main method
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println("Fetching block query...");
        int blockNumber = 69333000;
        System.out.println("blockNumber = " + blockNumber);
        String recordFileName = getRecordFileNameForBlock(blockNumber);
        System.out.println("recordFileName = " + recordFileName);
    }
}
