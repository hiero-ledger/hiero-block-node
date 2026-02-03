// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

/**
 * Constants used for downloading block data from Google Cloud Storage.
 *
 * <p>This utility class provides configuration values for accessing the Hedera mainnet
 * streams bucket, including bucket name, path prefix, and GCP project ID for requester-pays billing.
 */
public final class DownloadConstants {

    /** The name of the GCS bucket containing Hedera mainnet streams. */
    public static final String BUCKET_NAME = "hedera-mainnet-streams";

    /** The path prefix within the bucket for record streams. */
    public static final String BUCKET_PATH_PREFIX = "recordstreams/";

    /** The GCP project ID for requester-pays billing, read from GCP_PROJECT_ID environment variable. */
    public static final String GCP_PROJECT_ID = System.getenv().getOrDefault("GCP_PROJECT_ID", "myprojectid");

    /** Private constructor to prevent instantiation of this utility class. */
    private DownloadConstants() {
        // Utility class - do not instantiate
    }
}
