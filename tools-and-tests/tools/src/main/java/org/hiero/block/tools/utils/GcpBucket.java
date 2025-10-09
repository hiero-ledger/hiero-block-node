package org.hiero.block.tools.utils;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.transfermanager.DownloadResult;
import com.google.cloud.storage.transfermanager.ParallelDownloadConfig;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.google.cloud.storage.transfermanager.TransferManagerConfig;
import java.nio.file.Path;
import java.util.List;

public class GcpBucket {


    public static void downloadManyBlobs(
            String bucketName, List<BlobInfo> blobs, Path destinationDirectory) throws Exception {

        try (TransferManager transferManager =
                TransferManagerConfig.newBuilder().build().getService()) {
            ParallelDownloadConfig parallelDownloadConfig =
                    ParallelDownloadConfig.newBuilder()
                            .setBucketName(bucketName)
                            .setDownloadDirectory(destinationDirectory)
                            .build();

            List<DownloadResult> results =
                    transferManager.downloadBlobs(blobs, parallelDownloadConfig).getDownloadResults();

            for (DownloadResult result : results) {
                System.out.println(
                        "Download of "
                                + result.getInput().getName()
                                + " completed with status "
                                + result.getStatus());
            }
        }
    }
}
