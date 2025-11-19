// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.util.regex.Pattern;

public class MirrorNodeUtils {
    public static final String MAINNET_MIRROR_NODE_API_URL = "https://mainnet-public.mirrornode.hedera.com/api/v1/";
    public static final Pattern SYMANTIC_VERSION_PATTERN =
            Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$");

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

    /**
     * Parse a semantic version string like "0.136.0" into a long for comparison.
     *
     * @param versionStr the semantic version string
     * @return the parsed version as a long
     */
    public static long parseSymantecVersion(String versionStr) {
        String[] parts = versionStr.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid semantic version: " + versionStr);
        }
        long major = Long.parseLong(parts[0]);
        long minor = Long.parseLong(parts[1]);
        long patch = Long.parseLong(parts[2]);
        return (major << 32) | (minor << 16) | patch;
    }
}
