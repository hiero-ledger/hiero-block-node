// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the expanded cloud storage plugin.
 *
 * <p>Set {@code expanded.cloud.storage.endpointUrl} to a non-blank value to enable the plugin.
 * Leave it blank (the default) to disable the plugin at startup.
 *
 * @param endpointUrl      S3-compatible endpoint URL (e.g. {@code https://s3.amazonaws.com/});
 *                         empty string disables the plugin.
 * @param bucketName       name of the S3 bucket to upload blocks into.
 * @param objectKeyPrefix  prefix prepended to every object key (e.g. {@code "blocks"}).
 * @param storageClass     S3 storage class (e.g. {@code STANDARD}, {@code GLACIER}).
 * @param regionName       AWS / S3-compatible region name (e.g. {@code us-east-1}).
 * @param accessKey        S3 access key; not logged.
 * @param secretKey        S3 secret key; not logged.
 */
@ConfigData("expanded.cloud.storage")
public record ExpandedCloudStorageConfig(
        @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
        @Loggable @ConfigProperty(defaultValue = "block-node-blocks") String bucketName,
        @Loggable @ConfigProperty(defaultValue = "blocks") String objectKeyPrefix,
        @Loggable @ConfigProperty(defaultValue = "STANDARD") String storageClass,
        @Loggable @ConfigProperty(defaultValue = "us-east-1") String regionName,
        @ConfigProperty(defaultValue = "") String accessKey,
        @ConfigProperty(defaultValue = "") String secretKey) {}
