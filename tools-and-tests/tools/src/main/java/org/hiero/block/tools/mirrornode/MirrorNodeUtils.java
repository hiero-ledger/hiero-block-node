// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.regex.Pattern;

public class MirrorNodeUtils {
    public static final String MAINNET_MIRROR_NODE_API_URL = "https://mainnet-public.mirrornode.hedera.com/api/v1/";
    public static final Pattern SYMANTIC_VERSION_PATTERN =
            Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$");

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_RETRY_DELAY_MS = 2000;
    private static final Set<Integer> RETRYABLE_HTTP_CODES = Set.of(429, 500, 502, 503, 504);
    /// Path prefix that identifies mirror-node block-lookup endpoints
    /// (`/api/v1/blocks`, `/api/v1/blocks/{n}`, `/api/v1/blocks?block.number=gte:{n}`).
    /// A 404 on any of these is treated as a transient tip-gap and retried
    /// (see issue #3683). Any other 404 remains terminal.
    private static final String BLOCKS_ENDPOINT_PATH_MARKER = "/api/v1/blocks";
    // package-private for testing
    static final int NO_RESPONSE_CODE = -1;

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
            int responseCode = NO_RESPONSE_CODE;
            try {
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(30000);
                conn.setReadTimeout(30000);
                responseCode = conn.getResponseCode();

                if (responseCode == HttpURLConnection.HTTP_OK) {
                    try (Reader reader = new InputStreamReader(conn.getInputStream())) {
                        return new Gson().fromJson(reader, JsonObject.class);
                    }
                } else {
                    throw new IOException("HTTP " + responseCode + " for URL: " + url);
                }
            } catch (IOException e) {
                lastException = e;
                boolean retryable = isRetryableException(responseCode, e, url);
                if (attempt < MAX_RETRIES && retryable) {
                    long delay = INITIAL_RETRY_DELAY_MS * (1L << (attempt - 1));
                    System.err.println("[MirrorNode] " + e.getMessage() + ", retrying in " + (delay / 1000)
                            + "s (attempt " + attempt + "/" + MAX_RETRIES + ")...");
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting to retry", ie);
                    }
                } else if (!retryable) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        throw new UncheckedIOException(new IOException(
                "Failed after " + MAX_RETRIES + " retries: " + lastException.getMessage(), lastException));
    }

    // package-private for testing; two-arg overload kept for pre-existing call sites.
    static boolean isRetryableException(int responseCode, IOException e) {
        return isRetryableException(responseCode, e, null);
    }

    // package-private for testing
    static boolean isRetryableException(int responseCode, IOException e, URL url) {
        String msg = e.getMessage();
        return RETRYABLE_HTTP_CODES.contains(responseCode)
                || isTransientBlocksTipGap(responseCode, url)
                || e instanceof UnknownHostException
                || e instanceof SocketTimeoutException
                || e instanceof ConnectException
                || e instanceof NoRouteToHostException
                || (msg != null
                        && (msg.contains("Connection reset")
                                || msg.contains("Connection timed out")
                                || msg.contains("Broken pipe")));
    }

    /// Returns `true` when the response is `HTTP 404` on a mirror-node
    /// block-lookup endpoint. Mirror-node returns 404 briefly when the
    /// requested block number is at or just past the current indexer tip;
    /// re-issuing the request after a short backoff succeeds. Any 404 outside
    /// the `/api/v1/blocks` family stays terminal (see issue #3683).
    private static boolean isTransientBlocksTipGap(int responseCode, URL url) {
        return responseCode == HttpURLConnection.HTTP_NOT_FOUND
                && url != null
                && url.getPath() != null
                && url.getPath().startsWith(BLOCKS_ENDPOINT_PATH_MARKER);
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
