// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the expanded cloud storage plugin.
///
/// ## Required fields
/// `endpointUrl`, `bucketName`, `regionName`, `accessKey`, and `secretKey` must all be
/// explicitly set by the operator. The underlying bucky `S3Client` rejects a blank value for
/// any of them, so the plugin cannot create a client and skips all uploads until the values
/// are corrected and the node is restarted.
///
/// Each blank required field is reported as a WARNING naming the property (never its value)
/// during `init()`. This soft-disabled behaviour is temporary: once the block node supports
/// plugin-level health reporting, a missing required field will cause the plugin to report
/// unhealthy rather than silently skipping uploads.
///
/// ## Supplying credentials
/// `accessKey` and `secretKey` can be set directly in a config file, or supplied through the
/// environment. The block node maps every config property to an environment variable name
/// automatically (MicroProfile style), so `cloud.storage.expanded.accessKey` is settable as
/// `CLOUD_STORAGE_EXPANDED_ACCESS_KEY` and `cloud.storage.expanded.secretKey` as
/// `CLOUD_STORAGE_EXPANDED_SECRET_KEY`. Preferring the environment keeps credentials out of
/// config files on disk. The S3 client has no credential-chain or IAM instance-role support:
/// these two values are the only way to authenticate.
///
/// @param endpointUrl          S3-compatible endpoint URL (e.g. `https://s3.amazonaws.com/`).
///                             Required; must not be blank.
/// @param bucketName           name of the S3 bucket to upload blocks into. Required; must not
///                             be blank.
/// @param objectKeyPrefix      prefix prepended to every object key (e.g. `"blocks"`).
///                             Set to empty string for no prefix. The full key format is:
///                             `{prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd`.
/// @param storageClass         S3 storage class. Must be `STANDARD`. BN relies on
///                             bucket lifecycle policies rather than storage-class headers to
///                             move objects to archive tiers.
/// @param regionName           AWS / S3-compatible region name (e.g. `us-east-1`). Required;
///                             must not be blank.
/// @param accessKey            S3 access key; not logged. Required; must not be blank.
/// @param secretKey            S3 secret key; not logged. Required; must not be blank.
/// @param uploadTimeoutSeconds maximum seconds to wait for in-flight uploads during
///                             `stop()` before treating them as failed. Default: 60.
/// @param retryEnabled         whether failed uploads are held in memory and retried in the
///                             background instead of being reported as failed immediately.
///                             Blocks are never written to local disk; a process restart loses
///                             any not-yet-recovered retry. Default: `true`.
/// @param retryIntervalSeconds serves two purposes: the fixed period of the background retry
///                             tick, and the backoff applied to a block after each failed
///                             attempt. Each tick re-attempts every buffered block whose
///                             backoff has elapsed and that isn't already in flight.
///                             Default: 10.
/// @param retryMaxAgeSeconds   maximum time, in seconds, a block may remain buffered for retry
///                             before it is dropped and reported as a terminal failure.
///                             Default: 60.
/// @param retryMaxPendingBlocks maximum number of blocks held in the in-memory retry buffer at
///                             once. A new failure arriving at capacity evicts the
///                             longest-buffered block, which is then reported as a terminal
///                             failure, and the new block takes its place. Default: 30.
// spotless:off - long annotations on record components must stay on one line
@ConfigData("cloud.storage.expanded")
public record ExpandedCloudStorageConfig(
        @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
        @Loggable @ConfigProperty(defaultValue = "") String bucketName,
        @Loggable @ConfigProperty(defaultValue = "") String objectKeyPrefix,
        @Loggable @ConfigProperty(defaultValue = "STANDARD") StorageClass storageClass,
        @Loggable @ConfigProperty(defaultValue = "") String regionName,
        @ConfigProperty(defaultValue = "") String accessKey,
        @ConfigProperty(defaultValue = "") String secretKey,
        @Loggable @ConfigProperty(defaultValue = "60") @Min(1) int uploadTimeoutSeconds,
        @Loggable @ConfigProperty(defaultValue = "true") boolean retryEnabled,
        @Loggable @ConfigProperty(defaultValue = "10") @Min(1) int retryIntervalSeconds,
        @Loggable @ConfigProperty(defaultValue = "60") @Min(1) int retryMaxAgeSeconds,
        @Loggable @ConfigProperty(defaultValue = "30") @Min(1) int retryMaxPendingBlocks) {

    /// S3 storage class values accepted by this plugin.
    public enum StorageClass {
        STANDARD
    }
}
// spotless:on
