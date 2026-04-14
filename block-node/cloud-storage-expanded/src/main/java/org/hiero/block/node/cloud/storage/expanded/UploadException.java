// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

/// Checked exception thrown by {@link S3UploadClient#uploadFile} when the S3 service
/// returns an error response (e.g. 4xx / 5xx HTTP status, auth failure, or client
/// initialisation failure).
///
/// This exception isolates the bucky library's {@code S3ClientException} hierarchy
/// from the rest of the package. Only {@link BuckyS3UploadClient} imports bucky types;
/// all other classes catch {@code UploadException} instead.
///
/// {@link java.io.IOException} is kept as a separate signal for transport-level failures
/// (socket errors, stream interruption) so callers can distinguish S3 service errors
/// from I/O errors when setting {@link SingleBlockStoreTask.UploadStatus}.
class UploadException extends Exception {

    /// Wraps the underlying S3 client exception.
    ///
    /// @param message a human-readable description of the failure
    /// @param cause   the originating S3 client exception
    UploadException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
