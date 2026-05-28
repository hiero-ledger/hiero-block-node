// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

/**
 * Type of cloud storage bucket used for record streams.
 */
public enum BucketType {
    /** Google Cloud Storage bucket */
    GCS,

    /** Amazon S3 or S3-compatible bucket (e.g., MinIO) */
    S3
}
