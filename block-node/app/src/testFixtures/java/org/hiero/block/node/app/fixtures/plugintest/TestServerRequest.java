// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import io.helidon.common.context.Context;
import io.helidon.common.parameters.Parameters;
import io.helidon.common.socket.PeerInfo;
import io.helidon.common.uri.UriInfo;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.Header;
import io.helidon.http.HttpPrologue;
import io.helidon.http.RoutedPath;
import io.helidon.http.ServerRequestHeaders;
import io.helidon.http.media.ReadableEntity;
import io.helidon.webserver.ListenerContext;
import io.helidon.webserver.ProxyProtocolData;
import io.helidon.webserver.http.HttpSecurity;
import io.helidon.webserver.http.ServerRequest;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Minimal {@link ServerRequest} test double that exposes only a settable request path (via
 * {@link #path()}). All other operations are unsupported.
 */
public class TestServerRequest implements ServerRequest {
    private final String path;

    public TestServerRequest(String path) {
        this.path = path;
    }

    @Override
    public RoutedPath path() {
        return new TestRoutedPath(path);
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSecure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReadableEntity content() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String socketId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String serverSocketId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Context context() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenerContext listenerContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpSecurity security() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean continueSent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void streamFilter(UnaryOperator<InputStream> filterFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ProxyProtocolData> proxyProtocolData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpPrologue prologue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerRequestHeaders headers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UriQuery query() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PeerInfo remotePeer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PeerInfo localPeer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String authority() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void header(Header header) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int id() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UriInfo requestedUri() {
        throw new UnsupportedOperationException();
    }

    /** Minimal {@link RoutedPath} returning the configured path string. */
    private record TestRoutedPath(String path) implements RoutedPath {
        @Override
        public String rawPath() {
            return path;
        }

        @Override
        public String rawPathNoParams() {
            return path;
        }

        @Override
        public Parameters matrixParameters() {
            return Parameters.empty("matrix");
        }

        @Override
        public Parameters pathParameters() {
            return Parameters.empty("path");
        }

        @Override
        public RoutedPath absolute() {
            return this;
        }

        @Override
        public void validate() {
            // no-op
        }
    }
}
