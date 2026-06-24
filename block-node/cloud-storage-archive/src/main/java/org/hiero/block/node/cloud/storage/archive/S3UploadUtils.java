// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientInitializationException;
import com.hedera.bucky.S3ResponseException;
import java.io.IOException;
import java.util.List;

/// Shared S3 multipart-upload utilities used by [BlockUploadTask], [TempArchiveUploadTask],
/// [ConsolidationTask], and [StartupRecoveryTask].
final class S3UploadUtils {

    private static final System.Logger LOGGER = System.getLogger(S3UploadUtils.class.getName());

    static final String CONTENT_TYPE = "application/x-tar";

    private S3UploadUtils() {}

    /// Creates a new [S3Client] from the given [CloudStorageArchiveConfig].
    static S3Client createClient(CloudStorageArchiveConfig config) throws S3ClientInitializationException {
        return new S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(), config.accessKey(), config.secretKey());
    }

    /// Uploads `bytes` as the next numbered part of a multipart upload, records the returned ETag
    /// in `etags`, and logs the part number.  The part number is 1-based and derived from the
    /// current size of `etags`.
    static void uploadPart(byte[] bytes, String key, S3Client s3, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        final int partNumber = etags.size() + 1;
        final String etag = s3.multipartUploadPart(key, uploadId, partNumber, bytes);
        etags.add(etag);
        LOGGER.log(TRACE, "Uploaded part {0} for key {1}", partNumber, key);
    }

    /// Splits `buffer` at `partSize`: returns a [SplitBuffer] whose [SplitBuffer#part] contains
    /// the first `partSize` bytes and whose [SplitBuffer#remainder] contains the rest.
    /// `buffer` must be at least `partSize` bytes long.
    static SplitBuffer splitAtPartSize(byte[] buffer, int partSize) {
        final byte[] part = new byte[partSize];
        final byte[] remainder = new byte[buffer.length - partSize];
        System.arraycopy(buffer, 0, part, 0, partSize);
        System.arraycopy(buffer, partSize, remainder, 0, remainder.length);
        return new SplitBuffer(part, remainder);
    }

    record SplitBuffer(byte[] part, byte[] remainder) {}

    /// Returns a new array containing the bytes of `a` followed by the bytes of `b`.  Returns `b`
    /// when `a` is empty, and `a` when `b` is empty, to avoid an unnecessary allocation.
    static byte[] concat(byte[] a, byte[] b) {
        if (a.length == 0) {
            return b;
        }
        if (b.length == 0) {
            return a;
        }
        final byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    /// Aborts the multipart upload identified by `uploadId`, swallowing any exception into a DEBUG
    /// log entry.  Used in interruption and error paths where a best-effort abort is appropriate.
    static void abortQuietly(S3Client s3, String key, String uploadId) {
        try {
            s3.abortMultipartUpload(key, uploadId);
        } catch (S3ResponseException | IOException e) {
            LOGGER.log(DEBUG, "Failed to abort multipart upload for key {0}", key, e);
        }
    }
}
