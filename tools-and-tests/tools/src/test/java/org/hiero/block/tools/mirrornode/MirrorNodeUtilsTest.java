// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.isRetryableException;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
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
}
