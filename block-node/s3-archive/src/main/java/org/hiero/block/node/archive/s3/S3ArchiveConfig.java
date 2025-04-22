// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive.s3;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the archive service.
 *
 * @param blocksPerFile The number of blocks to write to a single archive file.
 * @param endpointUrl The endpoint URL for the archive service, e.g. "<a href="https://s3.amazonaws.com/">
 *                    https://s3.amazonaws.com/</a>".
 * @param bucketName The name of the bucket to store the archive files.
 * @param basePath The base path for the archive files within the bucket.
 * @param storageClass The storage class for uploaded blocks (e.g., STANDARD | REDUCED_REDUNDANCY | STANDARD_IA |
 *                     ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | OUTPOSTS | GLACIER_IR | SNOW |
 *                     EXPRESS_ONEZONE).
 * @param regionName The region name for the archive service (e.g., us-east-1).
 * @param accessKey The access key for the archive service.
 * @param secretKey The secret key for the archive service.
 */
@ConfigData("archive")
public record S3ArchiveConfig(
        @Loggable @ConfigProperty(defaultValue = "100000") int blocksPerFile,
        @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,
        @Loggable @ConfigProperty(defaultValue = "block-node-archive") String bucketName,
        @Loggable @ConfigProperty(defaultValue = "blocks") String basePath,
        @Loggable @ConfigProperty(defaultValue = "STANDARD") String storageClass,
        @Loggable @ConfigProperty(defaultValue = "us-east-1") String regionName,
        @ConfigProperty(defaultValue = "") String accessKey,
        @ConfigProperty(defaultValue = "") String secretKey) {
    /**
     * Constructor.
     */
    public S3ArchiveConfig {
        Preconditions.requirePositivePowerOf10(blocksPerFile);
    }
}
