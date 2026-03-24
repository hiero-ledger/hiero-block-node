// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.util.Set;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the expanded cloud storage plugin.
 *
 * <h2>Enabling the plugin</h2>
 * Set {@code expanded.cloud.storage.endpointUrl} to a non-blank value to enable the plugin.
 * Leave it blank (the default) to disable the plugin at startup.
 *
 * <h2>Credential options</h2>
 * Three credential strategies are supported, in priority order:
 * <ol>
 *   <li><b>Config properties</b> — set {@code expanded.cloud.storage.accessKey} and
 *       {@code expanded.cloud.storage.secretKey} directly. Swirlds Config supports
 *       environment-variable substitution: use {@code ${AWS_ACCESS_KEY_ID}} in the value
 *       to avoid embedding credentials in config files on disk.</li>
 *   <li><b>Environment variables</b> — if {@code accessKey} and {@code secretKey} are blank,
 *       the underlying S3 client falls back to the standard chain:
 *       {@code AWS_ACCESS_KEY_ID} / {@code AWS_SECRET_ACCESS_KEY}.</li>
 *   <li><b>IAM / instance role</b> — leave both fields blank and attach an IAM role with
 *       {@code s3:PutObject} on the bucket. This is the recommended approach for
 *       cloud-native (EC2 / ECS / GKE Workload Identity) deployments.</li>
 * </ol>
 *
 * @param endpointUrl          S3-compatible endpoint URL (e.g. {@code https://s3.amazonaws.com/});
 *                             empty string disables the plugin.
 * @param bucketName           name of the S3 bucket to upload blocks into.
 * @param objectKeyPrefix      prefix prepended to every object key (e.g. {@code "blocks"}).
 *                             Set to empty string for no prefix. The full key format is:
 *                             {@code {prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd}.
 * @param storageClass         S3 storage class. Must be {@code STANDARD}. BN relies on
 *                             bucket lifecycle policies rather than storage-class headers to
 *                             move objects to archive tiers. Validated at startup.
 * @param regionName           AWS / S3-compatible region name (e.g. {@code us-east-1}).
 * @param accessKey            S3 access key; not logged. Leave blank to use environment
 *                             variables or IAM instance role.
 * @param secretKey            S3 secret key; not logged. Leave blank to use environment
 *                             variables or IAM instance role.
 * @param uploadTimeoutSeconds maximum seconds to wait for a single block upload before
 *                             treating it as failed. Default: 60.
 */
@ConfigData("expanded.cloud.storage")
public record ExpandedCloudStorageConfig(
        @Loggable @ConfigProperty(defaultValue = "") String endpointUrl,

        @Loggable @ConfigProperty(defaultValue = "block-node-blocks")
        String bucketName,

        @Loggable @ConfigProperty(defaultValue = "blocks") String objectKeyPrefix,
        @Loggable @ConfigProperty(defaultValue = "STANDARD") String storageClass,

        @Loggable @ConfigProperty(defaultValue = "us-east-1")
        String regionName,

        @ConfigProperty(defaultValue = "") String accessKey,
        @ConfigProperty(defaultValue = "") String secretKey,
        @Loggable @ConfigProperty(defaultValue = "60") int uploadTimeoutSeconds) {

    /**
     * Valid S3 storage class values accepted by this plugin.
     * BN is scoped to STANDARD and relies on bucket lifecycle policies for archive tiers.
     * To expand this set, add options from the S3 API {@code x-amz-storage-class} header docs.
     */
    static final Set<String> VALID_STORAGE_CLASSES = Set.of("STANDARD");

    /**
     * Compact constructor that validates {@code storageClass} and {@code uploadTimeoutSeconds}
     * at construction time so misconfiguration fails fast at node startup.
     */
    public ExpandedCloudStorageConfig {
        if (!VALID_STORAGE_CLASSES.contains(storageClass)) {
            throw new IllegalArgumentException("Invalid expanded.cloud.storage.storageClass: '" + storageClass
                    + "'. Must be one of: " + VALID_STORAGE_CLASSES);
        }
        if (uploadTimeoutSeconds < 1) {
            throw new IllegalArgumentException(
                    "expanded.cloud.storage.uploadTimeoutSeconds must be >= 1, got: " + uploadTimeoutSeconds);
        }
    }
}
