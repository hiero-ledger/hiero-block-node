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
        final String url = buildBlocksQueryUrl(limit, order, timestampFilters);
        final JsonObject json = MirrorNodeUtils.readUrl(url);
        return parseBlocksResponse(json);
    }

    /**
     * Builds the URL for querying blocks from the mirror node API.
     *
     * @param limit number of blocks to retrieve
     * @param order ordering of blocks
     * @param timestampFilters optional list of timestamp filters
     * @return the complete query URL
     */
    private static String buildBlocksQueryUrl(
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
                if (ts != null && !ts.isBlank()) {
                    url.append("&timestamp=").append(URLEncoder.encode(ts, StandardCharsets.UTF_8));
                }
            }
        }

        return url.toString();
    }

    /**
     * Parses the JSON response containing block information.
     *
     * @param json the JSON response from the mirror node API
     * @return a list of BlockInfo objects
     */
    private static List<BlockInfo> parseBlocksResponse(JsonObject json) {
        List<BlockInfo> blocks = new ArrayList<>();

        if (json.has("blocks") && json.get("blocks").isJsonArray()) {
            json.getAsJsonArray("blocks").forEach(elem -> {
                blocks.add(parseBlockInfo(elem.getAsJsonObject()));
            });
        }
        return blocks;
    }

    /**
     * Parses a single block JSON object into a BlockInfo instance.
     *
     * @param blockJson the JSON object representing a block
     * @return a populated BlockInfo object
     */
    private static BlockInfo parseBlockInfo(JsonObject blockJson) {
        BlockInfo blockInfo = new BlockInfo();
        blockInfo.count = getIntOrDefault(blockJson, "count", 0);
        blockInfo.hapiVersion = getStringOrNull(blockJson, "hapi_version");
        blockInfo.hash = getStringOrNull(blockJson, "hash");
        blockInfo.name = getStringOrNull(blockJson, "name");
        blockInfo.number = getLongOrDefault(blockJson, "number", -1);
        blockInfo.previousHash = getStringOrNull(blockJson, "previous_hash");
        blockInfo.size = getLongOrDefault(blockJson, "size", 0);
        blockInfo.gasUsed = getLongOrDefault(blockJson, "gas_used", 0);
        parseTimestampInfo(blockJson, blockInfo);
        return blockInfo;
    }

    /**
     * Parses timestamp information from the block JSON and sets it on the BlockInfo.
     *
     * @param blockJson the JSON object representing a block
     * @param blockInfo the BlockInfo object to populate
     */
    private static void parseTimestampInfo(JsonObject blockJson, BlockInfo blockInfo) {
        if (blockJson.has("timestamp") && blockJson.get("timestamp").isJsonObject()) {
            JsonObject tsObj = blockJson.getAsJsonObject("timestamp");
            blockInfo.timestampFrom = getStringOrNull(tsObj, "from");
            blockInfo.timestampTo = getStringOrNull(tsObj, "to");
        }
    }

    /**
     * Safely extracts a string value from a JSON object, returning null if missing or null.
     *
     * @param json the JSON object
     * @param fieldName the field name
     * @return the string value or null
     */
    private static String getStringOrNull(JsonObject json, String fieldName) {
        return (json.has(fieldName) && !json.get(fieldName).isJsonNull())
                ? json.get(fieldName).getAsString()
                : null;
    }

    /**
     * Safely extracts an integer value from a JSON object, returning a default if missing.
     *
     * @param json the JSON object
     * @param fieldName the field name
     * @param defaultValue the default value
     * @return the integer value or default
     */
    private static int getIntOrDefault(JsonObject json, String fieldName, int defaultValue) {
        return json.has(fieldName) ? json.get(fieldName).getAsInt() : defaultValue;
    }

    /**
     * Safely extracts a long value from a JSON object, returning a default if missing or null.
     *
     * @param json the JSON object
     * @param fieldName the field name
     * @param defaultValue the default value
     * @return the long value or default
     */
    private static long getLongOrDefault(JsonObject json, String fieldName, long defaultValue) {
        return (json.has(fieldName) && !json.get(fieldName).isJsonNull())
                ? json.get(fieldName).getAsLong()
                : defaultValue;
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
