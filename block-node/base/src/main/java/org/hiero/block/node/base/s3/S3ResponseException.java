// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.w3c.dom.Document;

/**
 * A checked exception thrown when an S3 response is not successful.
 */
public class S3ResponseException extends S3ClientException {
    private final int statusCode;
    private final Document responseBody;

    public S3ResponseException(final int statusCode, @Nullable final Document responseBody) {
        super();
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public S3ResponseException(final int statusCode, @Nullable final Document responseBody, final String message) {
        super(message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public S3ResponseException(final int statusCode, @Nullable final Document responseBody, final Throwable cause) {
        super(cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public S3ResponseException(
            final int statusCode, @Nullable final Document responseBody, final String message, final Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Document getResponseBody() {
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
        if (responseBody != null) {
            // if there is a body, append standard information that we expect
            // from a S3 response (XML)
            sb.append(System.lineSeparator());
            sb.append("    Response error code: ").append(getRawContentOfTag(responseBody, "Code"));
            sb.append(System.lineSeparator());
            sb.append("    Response error message: ").append(getRawContentOfTag(responseBody, "Message"));
            sb.append(System.lineSeparator());
            sb.append("    Request ID: ").append(getRawContentOfTag(responseBody, "RequestId"));
        }
        return sb.toString();
    }

    private String getRawContentOfTag(final Document responseBody, final String tagName) {
        return responseBody
                .getDocumentElement()
                .getElementsByTagName(tagName)
                .item(0)
                .getTextContent();
    }
}
