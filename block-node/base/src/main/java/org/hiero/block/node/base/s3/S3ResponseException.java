// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.http.HttpHeaders;
import java.util.List;
import java.util.Map.Entry;

/**
 * A checked exception thrown when an S3 response is not successful.
 */
public final class S3ResponseException extends S3ClientException {
    private static final int MAX_RESPONSE_BODY_STRING_LENGTH = 2048;
    private static final int MAX_HEADER_STRING_LENGTH = 2048;
    private static final String FOUR_SPACE_INDENT = "    ";
    private final int statusCode;
    private final byte[] responseBody;
    private final HttpHeaders headers;

    public S3ResponseException(
            final int statusCode, @Nullable final byte[] responseBody, @Nullable final HttpHeaders headers) {
        super();
        this.statusCode = statusCode;
        this.responseBody = responseBody;
        this.headers = headers;
    }

    public S3ResponseException(
            final int statusCode,
            @Nullable final byte[] responseBody,
            @Nullable final HttpHeaders headers,
            final String message) {
        super(message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
        this.headers = headers;
    }

    public S3ResponseException(
            final int statusCode,
            @Nullable final byte[] responseBody,
            @Nullable final HttpHeaders headers,
            final Throwable cause) {
        super(cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
        this.headers = headers;
    }

    public S3ResponseException(
            final int statusCode,
            @Nullable final byte[] responseBody,
            @Nullable final HttpHeaders headers,
            final String message,
            final Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
        this.headers = headers;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public byte[] getResponseBody() {
        return responseBody;
    }

    public HttpHeaders getHeaders() {
        return headers;
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
        sb.append(FOUR_SPACE_INDENT).append("Response status code: ").append(statusCode);
        if (headers != null) {
            // for each header, append the header name and value(s)
            sb.append(System.lineSeparator());
            sb.append(FOUR_SPACE_INDENT).append("Response headers:");
            // we limit the size of the printed headers
            int headerSizeCount = 0;
            for (final Entry<String, List<String>> entry : headers.map().entrySet()) {
                // for each header, we get the key and values
                final String headerKey = entry.getKey() + ": ";
                sb.append(System.lineSeparator());
                if (headerSizeCount + headerKey.length() > MAX_HEADER_STRING_LENGTH) {
                    // if string limit size is exceeded, break
                    sb.append(FOUR_SPACE_INDENT.repeat(2)).append("...");
                    break;
                } else {
                    // append the header key
                    sb.append(FOUR_SPACE_INDENT.repeat(2)).append(headerKey);
                    headerSizeCount += headerKey.length();
                }
                // append the header values, usually we expect only one value per header
                final List<String> values = entry.getValue();
                boolean isFirstValue = true;
                for (final String value : values) {
                    // if string limit size is exceeded, break
                    if (headerSizeCount + value.length() > MAX_HEADER_STRING_LENGTH) {
                        // if the value size exceeds the limit, break
                        sb.append(" ...");
                        break;
                    } else {
                        // append the value, separate with a comma for multi-value headers
                        if (!isFirstValue) {
                            sb.append(", ");
                            headerSizeCount += 2;
                        } else {
                            isFirstValue = false;
                        }
                        sb.append(value);
                        headerSizeCount += value.length();
                    }
                }
            }
        }
        if (responseBody != null && responseBody.length > 0) {
            // if there is a response body, append it
            sb.append(System.lineSeparator());
            // we limit the size of the printed response body
            sb.append("    Response body: ");
            if (responseBody.length > MAX_RESPONSE_BODY_STRING_LENGTH) {
                sb.append(new String(responseBody, 0, MAX_RESPONSE_BODY_STRING_LENGTH))
                        .append(" ...");
            } else {
                // if the response body is small enough, append it fully
                sb.append(new String(responseBody));
            }
        }
        return sb.toString();
    }
}
