// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import java.io.IOException;
import java.util.Iterator;

/// Minimal abstraction over S3 upload operations used by this plugin.
///
/// The production implementation is {@link BuckyS3UploadClient}. Package-private
/// implementations in the test source tree implement {@link #uploadFile} to capture or
/// throw as needed — no Docker or real S3 endpoint required for unit tests.
interface S3UploadClient extends AutoCloseable {

    /// Uploads content to S3 using multipart upload.
    ///
    /// @param objectKey       the key for the object in S3
    /// @param storageClass    the S3 storage class (e.g., `"STANDARD"`)
    /// @param contentIterable an iterator of byte-array chunks representing the file content
    /// @param contentType     the MIME content type (e.g., `"application/octet-stream"`)
    /// @throws UploadException if the S3 service returns an error response
    /// @throws IOException     if a transport-level I/O error occurs
    void uploadFile(String objectKey, String storageClass, Iterator<byte[]> contentIterable, String contentType)
            throws UploadException, IOException;

    /// Closes the underlying S3 client and releases any held resources.
    ///
    /// Narrows the throws clause of {@link AutoCloseable#close()} — implementations in this
    /// package must not throw checked exceptions from {@code close()}.
    @Override
    void close();
}
