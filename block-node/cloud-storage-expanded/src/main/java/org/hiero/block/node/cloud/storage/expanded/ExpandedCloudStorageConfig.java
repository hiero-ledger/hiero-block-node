// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the expanded cloud storage plugin.
///
/// ## Required fields
/// `endpointUrl`, `bucketName`, and `regionName` must all be explicitly set by the operator.
/// A blank value for any of them is a misconfiguration. The plugin logs a WARNING for each
/// blank required field at initialisation time and skips all uploads until the values are
/// corrected and the node is restarted. This soft-disabled behaviour is temporary — once the
/// block node supports plugin-level health reporting, a missing required field will cause the
/// plugin to report unhealthy rather than silently skipping uploads.
///
/// ## Credential options
/// Three credential strategies are supported, in priority order:
///
/// 1. **Config properties** — set `cloud.storage.expanded.accessKey` and
///    `cloud.storage.expanded.secretKey` directly. Swirlds Config supports
///    environment-variable substitution: use `${CLOUD_EXPANDED_ACCESS_KEY}` in the value
///    to avoid embedding credentials in config files on disk.
/// 2. **Environment variables** — if `accessKey` and `secretKey` are blank,
///    the underlying S3 client falls back to the standard chain:
///    `CLOUD_EXPANDED_ACCESS_KEY` / `CLOUD_EXPANDED_SECRET_KEY`.
/// 3. **IAM / instance role** — leave both fields blank and attach an IAM role with
///    `s3:PutObject` on the bucket. This is the recommended approach for
///    cloud-native (EC2 / ECS / GKE Workload Identity) deployments.
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
/// @param accessKey            S3 access key; not logged. Leave blank to use environment
///                             variables or IAM instance role.
/// @param secretKey            S3 secret key; not logged. Leave blank to use environment
///                             variables or IAM instance role.
/// @param uploadTimeoutSeconds maximum seconds to wait for in-flight uploads during
///                             `stop()` before treating them as failed. Default: 60.
/// @param retryEnabled         whether failed uploads are held in memory and retried in the
///                             background instead of being reported as failed immediately.
///                             Blocks are never written to local disk; a process restart loses
///                             any not-yet-recovered retry. Default: `true`.
/// @param retryIntervalSeconds fixed interval, in seconds, at which the background retry tick
///                             re-attempts every currently-buffered block that isn't already
///                             in flight.
/// @param retryMaxAgeSeconds   maximum time, in seconds, a block may remain buffered for retry
///                             before it is dropped and reported as a terminal failure.
/// @param retryMaxPendingBlocks maximum number of blocks held in the in-memory retry buffer at
///                             once; a failure that would exceed this cap is reported as a
///                             terminal failure immediately instead of being buffered.
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
