// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the cloud storage download plugin, which recovers missing blocks from
/// the bucket the consensus nodes upload blocks to.
///
/// @param enabled              whether the plugin runs at all. Default: `false`.
/// @param endpointUrl          S3-compatible endpoint URL. Required when enabled.
/// @param regionName           S3 region name. Required when enabled.
/// @param bucketName           name of the bucket to read blocks from. Required when enabled.
/// @param accessKey            S3 access key; not logged. Required when enabled.
/// @param secretKey            S3 secret key; not logged. Required when enabled.
/// @param objectKeyPrefix      prefix in front of every object key, the network folder of the
///                             bucket, such as `mainnet` or `testnet-202611011200`. Changes at
///                             every network reset. Empty for no prefix.
/// @param pollIntervalMs       time between two probes of the bucket, in milliseconds.
/// @param aheadThresholdBlocks how many blocks the bucket must be ahead of the last stored
///                             block before the plugin pulls. Must be positive when enabled.
/// @param downloadWindow       number of blocks downloaded in parallel. Must be positive when
///                             enabled.
/// @param persistWaitTimeoutMs maximum time to wait for one submitted block to be persisted,
///                             in milliseconds.
/// @param requestTimeoutMs     maximum time to wait for the response headers of one bucket
///                             request, in milliseconds.
// spotless:off - long annotations on record components must stay on one line
@ConfigData("cloud.storage.download")
public record CloudStorageDownloadConfig(
        @Loggable @ConfigProperty(defaultValue = "false") boolean enabled,
        @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
        @Loggable @ConfigProperty(defaultValue = "") String regionName,
        @Loggable @ConfigProperty(defaultValue = "") String bucketName,
        @ConfigProperty(defaultValue = "") String accessKey,
        @ConfigProperty(defaultValue = "") String secretKey,
        @Loggable @ConfigProperty(defaultValue = "") String objectKeyPrefix,
        @Loggable @ConfigProperty(defaultValue = "5000") @Min(1) long pollIntervalMs,
        @Loggable @ConfigProperty(defaultValue = "0") long aheadThresholdBlocks,
        @Loggable @ConfigProperty(defaultValue = "10") int downloadWindow,
        @Loggable @ConfigProperty(defaultValue = "90000") @Min(1) long persistWaitTimeoutMs,
        @Loggable @ConfigProperty(defaultValue = "10000") @Min(1) long requestTimeoutMs) {}
// spotless:on
