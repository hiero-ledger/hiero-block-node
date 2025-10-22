// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

import static org.hiero.block.tools.commands.mirrornode.MirrorNodeUtils.MAINNET_MIRROR_NODE_API_URL;

import com.google.gson.JsonObject;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.HexFormat;

/**
 * Query Mirror Node and fetch block information
 */
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
