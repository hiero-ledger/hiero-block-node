// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static io.helidon.http.HeaderValues.CONNECTION_CLOSE;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

/// Adds `Connection: close` to a response, but only when the request came in over HTTP/1.1.
///
/// HTTP/2 forbids the `Connection` header outright (RFC 9113 8.2.2); Helidon throws
/// `Http2Exception: Connection in response headers` if a handler sets it on an HTTP/2 response.
/// The idle-connection problem this header works around is also HTTP/1.1-specific: keep-alive
/// sockets held open by probes, not HTTP/2's single multiplexed connection.
final class HttpConnectionSupport {
    private static final String HTTP_1_1 = "1.1";

    private HttpConnectionSupport() {}

    @NonNull
    static ServerResponse closeAfterHttp1(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        return HTTP_1_1.equals(req.prologue().protocolVersion()) ? res.header(CONNECTION_CLOSE) : res;
    }
}
