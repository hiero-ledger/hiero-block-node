// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Set;
import java.util.regex.Pattern;

public class MirrorNodeUtils {
    public static final String MAINNET_MIRROR_NODE_API_URL = "https://mainnet-public.mirrornode.hedera.com/api/v1/";
    public static final Pattern SYMANTIC_VERSION_PATTERN =
            Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$");

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_RETRY_DELAY_MS = 2000;
    private static final Set<Integer> RETRYABLE_HTTP_CODES = Set.of(429, 500, 502, 503, 504);

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
     * Read a URL and return the JSON object with retry logic for transient errors.
     *
     * @param url the URL to read
     * @return the JSON object
     */
    public static JsonObject readUrl(URL url) {
        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(30000);
                conn.setReadTimeout(30000);
                int responseCode = conn.getResponseCode();

                if (responseCode == HttpURLConnection.HTTP_OK) {
                    try (Reader reader = new InputStreamReader(conn.getInputStream())) {
                        return new Gson().fromJson(reader, JsonObject.class);
                    }
                } else if (RETRYABLE_HTTP_CODES.contains(responseCode)) {
                    lastException = new IOException("HTTP " + responseCode + " for URL: " + url);
                    if (attempt < MAX_RETRIES) {
                        long delay = INITIAL_RETRY_DELAY_MS * (1L << (attempt - 1)); // Exponential backoff
                        System.err.println("[MirrorNode] HTTP " + responseCode + ", retrying in " + (delay / 1000)
                                + "s (attempt " + attempt + "/" + MAX_RETRIES + ")...");
                        Thread.sleep(delay);
                    }
                } else {
                    throw new IOException("HTTP " + responseCode + " for URL: " + url);
                }
            } catch (IOException e) {
                lastException = e;
                if (attempt < MAX_RETRIES && isRetryableException(e)) {
                    long delay = INITIAL_RETRY_DELAY_MS * (1L << (attempt - 1));
                    System.err.println("[MirrorNode] " + e.getMessage() + ", retrying in " + (delay / 1000)
                            + "s (attempt " + attempt + "/" + MAX_RETRIES + ")...");
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ie);
                    }
                } else if (!isRetryableException(e)) {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException(
                "Failed after " + MAX_RETRIES + " retries: " + lastException.getMessage(), lastException);
    }

    private static boolean isRetryableException(IOException e) {
        String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("503")
                || msg.contains("502")
                || msg.contains("500")
                || msg.contains("504")
                || msg.contains("429")
                || msg.contains("Connection reset")
                || msg.contains("Connection timed out");
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
