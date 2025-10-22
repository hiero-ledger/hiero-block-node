// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URL;

public class MirrorNodeUtils {
    public static final String MAINNET_MIRROR_NODE_API_URL = "https://mainnet-public.mirrornode.hedera.com/api/v1/";

    /**
     * Read a URL and return the JSON object.
     *
     * @param url the URL to read
     * @return the JSON object
     */
    public static JsonObject readUrl(String url) {
        try {
            return readUrl(new URI(url).toURL());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read a URL and return the JSON object.
     *
     * @param url the URL to read
     * @return the JSON object
     */
    public static JsonObject readUrl(URL url) {
        try {
            try (Reader reader = new InputStreamReader(url.openStream())) {
                return new Gson().fromJson(reader, JsonObject.class);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
