// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

public class DownloadConstants {
    public static final String BUCKET_NAME = "hedera-mainnet-streams";
    public static final String BUCKET_PATH_PREFIX = "recordstreams/";
    // Get the GCP project ID for requester pays
    public static final String GCP_PROJECT_ID = System.getenv().getOrDefault("GCP_PROJECT_ID", "myprojectid");
}
