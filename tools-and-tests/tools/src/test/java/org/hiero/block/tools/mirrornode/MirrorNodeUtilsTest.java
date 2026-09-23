// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.NO_RESPONSE_CODE;
import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.isRetryableException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Tests for {@link MirrorNodeUtils}. */
class MirrorNodeUtilsTest {

    private static final String URL = "https://mainnet-public.mirrornode.hedera.com/api/v1/blocks";

    @Test
    void transientNetworkExceptionsAreRetryable() {
        // regression for #3644: the message is only the hostname, no retry substring
        assertTrue(isRetryableException(
                NO_RESPONSE_CODE, new UnknownHostException("mainnet-public.mirrornode.hedera.com")));
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new SocketTimeoutException("Read timed out")));
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new SocketTimeoutException("Connect timed out")));
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new ConnectException("Connection refused")));
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new NoRouteToHostException("No route to host")));
    }

    @Test
    void retryableHttpStatusesAreRetryable() {
        for (int status : new int[] {429, 500, 502, 503, 504}) {
            assertTrue(isRetryableException(status, new IOException("HTTP " + status + " for URL: " + URL)));
        }
    }

    @Test
    void retryableMessagesAreRetryable() {
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new IOException("Connection reset")));
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new IOException("Connection timed out")));
        assertTrue(isRetryableException(NO_RESPONSE_CODE, new IOException("Broken pipe")));
    }

    @Test
    void nonTransientExceptionsAreNotRetryable() {
        assertFalse(isRetryableException(NO_RESPONSE_CODE, new IOException("Stream closed")));
        assertFalse(isRetryableException(NO_RESPONSE_CODE, new IOException()));
        // 404 with no URL context (two-arg overload) stays terminal
        assertFalse(isRetryableException(404, new IOException("HTTP 404 for URL: " + URL)));
        assertFalse(isRetryableException(404, new IOException("HTTP 404 for URL: " + URL + "/5000")));
    }

    @Test
    void notFoundOnBlocksEndpointIsRetryable() throws Exception {
        // #3683: mirror-node returns a transient 404 when a requested block is at or
        // just past the indexer tip. Retry the /api/v1/blocks family only.
        URL blocksRange = new URI(URL + "?block.number=gte:100305505&limit=100&order=asc").toURL();
        URL blocksById = new URI(URL + "/5000").toURL();
        assertTrue(isRetryableException(404, new IOException("HTTP 404 for URL: " + blocksRange), blocksRange));
        assertTrue(isRetryableException(404, new IOException("HTTP 404 for URL: " + blocksById), blocksById));
    }

    @Test
    void notFoundOnNonBlocksEndpointStaysTerminal() throws Exception {
        // 404 outside /api/v1/blocks family is a real not-found and must not retry
        URL tokens = new URI("https://mainnet-public.mirrornode.hedera.com/api/v1/tokens/1").toURL();
        URL accounts = new URI("https://mainnet-public.mirrornode.hedera.com/api/v1/accounts/1").toURL();
        assertFalse(isRetryableException(404, new IOException("HTTP 404 for URL: " + tokens), tokens));
        assertFalse(isRetryableException(404, new IOException("HTTP 404 for URL: " + accounts), accounts));
    }

    @Test
    void notFoundOnBlocksEndpointRetriesAndEventuallySucceeds() throws Exception {
        // Server returns 404 once, then a 200 with a JSON body — the retry loop should
        // reach the 200 (two requests total) instead of throwing on the first 404.
        // One retry keeps the exponential backoff wait to its initial delay (~2s).
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/api/v1/blocks", exchange -> {
            int n = requests.incrementAndGet();
            if (n < 2) {
                exchange.sendResponseHeaders(404, -1);
            } else {
                byte[] body = "{\"blocks\":[]}".getBytes();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        try {
            URL url = new URI("http://localhost:" + server.getAddress().getPort()
                            + "/api/v1/blocks?block.number=gte:100305505&limit=100&order=asc")
                    .toURL();
            var json = MirrorNodeUtils.readUrl(url);
            assertEquals(2, requests.get());
            assertTrue(json.has("blocks"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void notFoundOnNonBlocksEndpointFailsWithoutRetry() throws Exception {
        // A single 404 on a non-blocks path stays terminal — no retry.
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/api/v1/tokens", exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
        try {
            URL url = new URI("http://localhost:" + server.getAddress().getPort() + "/api/v1/tokens/1").toURL();
            assertThrows(UncheckedIOException.class, () -> MirrorNodeUtils.readUrl(url));
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }
}
