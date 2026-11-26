// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Configuration for the cloud storage download plugin, which recovers missing blocks from
/// the bucket the consensus nodes upload blocks to. Loading the plugin with the bucket settings
/// turns it on; there is no switch of its own. Without them the plugin stays idle.
///
/// @param endpointUrl          S3-compatible endpoint URL. Required.
/// @param regionName           S3 region name. Required.
/// @param bucketName           name of the bucket to read blocks from. Required.
/// @param accessKey            S3 access key; not logged. Required.
/// @param secretKey            S3 secret key; not logged. Required.
/// @param objectKeyPrefix      prefix in front of every object key, the network folder of the
///                             bucket, such as `mainnet` or `testnet-202611011200`. Changes at
///                             every network reset. Empty for no prefix.
/// @param pollIntervalMs       time between two probes of the bucket, in milliseconds.
///                             Minimum 1.
/// @param aheadThresholdBlocks how many blocks the bucket must be ahead of the last stored
///                             block before the plugin pulls. Minimum 1.
/// @param downloadWindow       number of blocks downloaded in parallel. Minimum 1.
/// @param persistWaitTimeoutMs maximum time to wait for one submitted block to be persisted,
///                             in milliseconds. Minimum 1.
/// @param requestTimeoutMs     maximum time to wait for the response headers of one bucket
///                             request, in milliseconds. Minimum 1.
// spotless:off - long annotations on record components must stay on one line
@ConfigData("cloud.storage.download")
public record CloudStorageDownloadConfig(
        @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
        @Loggable @ConfigProperty(defaultValue = "") String regionName,
        @Loggable @ConfigProperty(defaultValue = "") String bucketName,
        @ConfigProperty(defaultValue = "") String accessKey,
        @ConfigProperty(defaultValue = "") String secretKey,
        @Loggable @ConfigProperty(defaultValue = "") String objectKeyPrefix,
        @Loggable @ConfigProperty(defaultValue = "10000") @Min(1) long pollIntervalMs,
        @Loggable @ConfigProperty(defaultValue = "10") @Min(1) long aheadThresholdBlocks,
        @Loggable @ConfigProperty(defaultValue = "10") @Min(1) int downloadWindow,
        @Loggable @ConfigProperty(defaultValue = "90000") @Min(1) long persistWaitTimeoutMs,
        @Loggable @ConfigProperty(defaultValue = "10000") @Min(1) long requestTimeoutMs) {}
// spotless:on
