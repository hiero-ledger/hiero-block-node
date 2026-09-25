// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.isRetryableException;
import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.isTransientBlocksTipGap;
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
        assertTrue(isRetryableException(new UnknownHostException("mainnet-public.mirrornode.hedera.com")));
        assertTrue(isRetryableException(new SocketTimeoutException("Read timed out")));
        assertTrue(isRetryableException(new SocketTimeoutException("Connect timed out")));
        assertTrue(isRetryableException(new ConnectException("Connection refused")));
        assertTrue(isRetryableException(new NoRouteToHostException("No route to host")));
    }

    @Test
    void retryableMessagesAreRetryable() {
        // same message shape readUrl builds for non-OK HTTP responses
        assertTrue(isRetryableException(new IOException("HTTP 503 for URL: " + URL)));
        assertTrue(isRetryableException(new IOException("HTTP 429 for URL: " + URL)));
        assertTrue(isRetryableException(new IOException("Connection reset")));
        assertTrue(isRetryableException(new IOException("Broken pipe")));
    }

    @Test
    void nonTransientExceptionsAreNotRetryable() {
        assertFalse(isRetryableException(new IOException("HTTP 404 for URL: " + URL)));
        assertFalse(isRetryableException(new IOException()));
    }

    @Test
    void notFoundOnBlocksEndpointIsTransient() throws Exception {
        // #3683: mirror-node returns a transient 404 when a requested block is at or
        // just past the indexer tip. Retry the /api/v1/blocks family only.
        URL blocksRange = new URI(URL + "?block.number=gte:100305505&limit=100&order=asc").toURL();
        URL blocksById = new URI(URL + "/5000").toURL();
        assertTrue(isTransientBlocksTipGap(404, blocksRange));
        assertTrue(isTransientBlocksTipGap(404, blocksById));
    }

    @Test
    void notFoundOnNonBlocksEndpointStaysTerminal() throws Exception {
        URL tokens = new URI("https://mainnet-public.mirrornode.hedera.com/api/v1/tokens/1").toURL();
        URL accounts = new URI("https://mainnet-public.mirrornode.hedera.com/api/v1/accounts/1").toURL();
        assertFalse(isTransientBlocksTipGap(404, tokens));
        assertFalse(isTransientBlocksTipGap(404, accounts));
        // non-404 codes never tip-gap
        assertFalse(isTransientBlocksTipGap(500, tokens));
        assertFalse(isTransientBlocksTipGap(200, tokens));
    }

    @Test
    void notFoundOnBlocksEndpointRetriesAndEventuallySucceeds() throws Exception {
        // Server returns 404 once, then a 200 with a JSON body — the retry loop should
        // reach the 200 (two requests total) instead of throwing on the first 404.
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
