// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

@ConfigData("cloud-archive")
public record ArchiveCloudStorageConfig(
        // spotless:off
    @Loggable @ConfigProperty(defaultValue = "5") @Max(6) @Min(1) int groupingLevel,
    @Loggable @ConfigProperty(defaultValue = "10") @Max(2047) @Min(5) int partSizeMb,
    @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
    @Loggable @ConfigProperty(defaultValue = "") String regionName,
    @ConfigProperty(defaultValue = "") String accessKey,
    @ConfigProperty(defaultValue = "") String secretKey,
    @Loggable @ConfigProperty(defaultValue = "") String bucketName,
    @Loggable @ConfigProperty(defaultValue = "STANDARD") S3StorageClass storageClass) {
    // spotless:on

    // Source for storage class values: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
    enum S3StorageClass {
        STANDARD,
        STANDARD_IA,
        INTELLIGENT_TIERING,
        ONEZONE_IA,
        EXPRESS_ONEZONE,
        GLACIER,
        GLACIER_IR,
        DEEP_ARCHIVE,
        REDUCED_REDUNDANCY
    }
}
