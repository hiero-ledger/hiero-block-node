package org.hiero.block.tools.commands.days.download;

import com.google.cloud.storage.StorageOptions;

public class DownloadConstants {
    public static final String BUCKET_NAME = "hedera-mainnet-streams";
    public static final String BUCKET_PATH_PREFIX = "recordstreams/";
    public static final long RECORD_FILES_PER_DAY = 30*60*24; // every 2 seconds
    // Get the GCP project ID for requester pays
    public static final String GCP_PROJECT_ID = System.getenv()
            .getOrDefault("GCP_PROJECT_ID", "myprojectid");
    // Create StorageOptions with userProject for requester pays
    public static final StorageOptions REQUESTER_PAYS_STORAGE_OPTIONS = StorageOptions.newBuilder()
            .setProjectId(GCP_PROJECT_ID)
            .build();
}
