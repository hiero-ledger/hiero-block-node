// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A checked exception thrown when an S3 response is not successful.
 */
public class S3ResponseException extends S3ClientException {
    private final int statusCode;
    private final byte[] responseBody;

    public S3ResponseException(final int statusCode, @Nullable final byte[] responseBody) {
        super();
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public S3ResponseException(final int statusCode, @Nullable final byte[] responseBody, final String message) {
        super(message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public S3ResponseException(final int statusCode, @Nullable final byte[] responseBody, final Throwable cause) {
        super(cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public S3ResponseException(
            final int statusCode, @Nullable final byte[] responseBody, final String message, final Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public byte[] getResponseBody() {
        return responseBody;
    }

    @Override
    public String toString() {
        // start with standard exception string building
        final String className = getClass().getName();
        final String message = getLocalizedMessage();
        final StringBuilder sb = new StringBuilder(className);
        if (message != null) {
            // if there is a message, append it
            sb.append(": ").append(message);
        }
        sb.append(System.lineSeparator());
        // append the response status code
        sb.append("    Response status code: ").append(statusCode);
        if (responseBody != null && responseBody.length > 0) {
            // if there is a response body, append it
            sb.append(System.lineSeparator());
            sb.append("    Response body: ").append(new String(responseBody));
        }
        return sb.toString();
    }
}
