// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import io.helidon.common.uri.UriQuery;
import io.helidon.http.Header;
import io.helidon.http.HeaderName;
import io.helidon.http.HeaderNames;
import io.helidon.http.ServerResponseHeaders;
import io.helidon.http.ServerResponseTrailers;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerResponse;
import java.io.OutputStream;
import java.util.function.UnaryOperator;

/**
 * Minimal {@link ServerResponse} test double that records the status code, the {@code Content-Type}
 * header, and the sent entity, exposing them via accessors. All other operations are unsupported.
 */
public class TestServerResponse implements ServerResponse {
    private int statusCode = -1;
    private String contentType;
    private Object sentEntity;
    private boolean sent;

    /** @return the status code passed to {@code status(...)}, or {@code -1} if unset */
    public int sentStatus() {
        return statusCode;
    }

    /** @return the value of the {@code Content-Type} header, or {@code null} if unset */
    public String contentType() {
        return contentType;
    }

    /** @return the entity passed to {@code send(...)}, or {@code null} if nothing was sent */
    public Object sentEntity() {
        return sentEntity;
    }

    @Override
    public ServerResponse status(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    @Override
    public ServerResponse status(Status status) {
        this.statusCode = status.code();
        return this;
    }

    @Override
    public Status status() {
        return Status.create(statusCode);
    }

    @Override
    public ServerResponse header(HeaderName name, String... values) {
        if (name.equals(HeaderNames.CONTENT_TYPE) && values.length > 0) {
            this.contentType = values[0];
        }
        return this;
    }

    @Override
    public ServerResponse header(Header header) {
        if (header.headerName().equals(HeaderNames.CONTENT_TYPE)) {
            this.contentType = header.values();
        }
        return this;
    }

    @Override
    public void send(Object entity) {
        this.sentEntity = entity;
        this.sent = true;
    }

    @Override
    public void send() {
        this.sent = true;
    }

    @Override
    public void send(byte[] bytes) {
        this.sentEntity = bytes;
        this.sent = true;
    }

    @Override
    public boolean isSent() {
        return sent;
    }

    @Override
    public OutputStream outputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long bytesWritten() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerResponse whenSent(Runnable listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerResponse reroute(String newPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerResponse reroute(String path, UriQuery query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerResponse next() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerResponseHeaders headers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerResponseTrailers trailers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void streamResult(String result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void streamFilter(UnaryOperator<OutputStream> filterFunction) {
        throw new UnsupportedOperationException();
    }
}
