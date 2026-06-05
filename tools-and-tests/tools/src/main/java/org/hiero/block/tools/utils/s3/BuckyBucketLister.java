// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.s3;

import static org.hiero.block.tools.config.NetworkConfig.current;

import com.hedera.bucky.S3Client;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.tools.records.ChainFile;
import org.hiero.block.tools.records.RecordFileDates;
import org.hiero.block.tools.utils.BucketLister;

/**
 * S3-compatible bucket lister using Hedera Bucky client.
 *
 * <p>Provides file listing capabilities for S3-compatible storage (AWS S3, GCS S3-compatible API,
 * MinIO, etc.) using HMAC credentials.
 *
 * <p>This implementation uses Hedera's Bucky library which provides a lightweight S3 client
 * that works with any S3-compatible endpoint.
 */
public final class BuckyBucketLister implements BucketLister {

    private final S3Client s3Client;
    private final String bucketName;
    private final int minNodeAccountId;
    private final int maxNodeAccountId;

    /**
     * Creates a new Bucky-based S3 bucket lister.
     *
     * @param endpoint S3 endpoint URL (e.g., "https://storage.googleapis.com")
     * @param accessKey S3/HMAC access key
     * @param secretKey S3/HMAC secret key
     * @param bucketName S3 bucket name
     * @param minNodeAccountId minimum node account ID
     * @param maxNodeAccountId maximum node account ID
     */
    public BuckyBucketLister(
            final String endpoint,
            final String accessKey,
            final String secretKey,
            final String bucketName,
            final int minNodeAccountId,
            final int maxNodeAccountId) {
        this.bucketName = bucketName;
        this.minNodeAccountId = minNodeAccountId;
        this.maxNodeAccountId = maxNodeAccountId;

        try {
            // Bucky S3Client constructor: S3Client(regionName, endpointUrl, bucketName, accessKey, secretKey)
            // Region is not critical for GCS S3-compatible API
            this.s3Client = new S3Client("us-east-1", endpoint, bucketName, accessKey, secretKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Bucky S3 client", e);
        }
    }

    /**
     * Creates a Bucky bucket lister from environment variables.
     *
     * <p>Reads configuration from:
     * <ul>
     *   <li>GCS_ENDPOINT or defaults to "https://storage.googleapis.com"</li>
     *   <li>GCS_HMAC_ACCESS_ID - required</li>
     *   <li>GCS_HMAC_SECRET - required</li>
     *   <li>GCS_BUCKET_NAME - required</li>
     * </ul>
     *
     * @param minNodeAccountId minimum node account ID
     * @param maxNodeAccountId maximum node account ID
     * @return configured BuckyBucketLister
     * @throws IllegalArgumentException if required environment variables are missing
     */
    public static BuckyBucketLister fromEnvironment(final int minNodeAccountId, final int maxNodeAccountId) {
        final String endpoint = System.getenv().getOrDefault("GCS_ENDPOINT", "https://storage.googleapis.com");
        final String accessKey = System.getenv("GCS_HMAC_ACCESS_ID");
        final String secretKey = System.getenv("GCS_HMAC_SECRET");
        final String bucketName = System.getenv("GCS_BUCKET_NAME");

        if (accessKey == null || accessKey.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: GCS_HMAC_ACCESS_ID");
        }
        if (secretKey == null || secretKey.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: GCS_HMAC_SECRET");
        }
        if (bucketName == null || bucketName.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: GCS_BUCKET_NAME");
        }

        System.out.println("[BuckyBucketLister] Using S3-compatible access with endpoint: " + endpoint);
        System.out.println("[BuckyBucketLister] Bucket: " + bucketName);

        return new BuckyBucketLister(endpoint, accessKey, secretKey, bucketName, minNodeAccountId, maxNodeAccountId);
    }

    @Override
    public List<ChainFile> listDay(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the day (e.g., "2024-01-01")
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        final String bucketPrefix = current().bucketPathPrefix();

        System.out.println("[BuckyBucketLister] Listing files for day: " + dayPrefix);

        final List<ChainFile> allFiles = new ArrayList<>();
        int failedNodes = 0;

        // List files for each node
        for (int nodeId = minNodeAccountId; nodeId <= maxNodeAccountId; nodeId++) {
            try {
                // List record files
                String recordPrefix = bucketPrefix + "record0.0." + nodeId + "/" + dayPrefix;
                allFiles.addAll(listObjectsWithPrefix(recordPrefix, nodeId));

                // List sidecar files
                String sidecarPrefix = bucketPrefix + "record0.0." + nodeId + "/sidecar/" + dayPrefix;
                allFiles.addAll(listObjectsWithPrefix(sidecarPrefix, nodeId));
            } catch (Exception e) {
                failedNodes++;
                System.err.println(
                        "[BuckyBucketLister] Failed to list files for node " + nodeId + ": " + e.getMessage());
            }
        }

        // Fail if all nodes failed to list
        int totalNodes = maxNodeAccountId - minNodeAccountId + 1;
        if (failedNodes == totalNodes) {
            throw new RuntimeException("Failed to list files for all nodes on day: " + dayPrefix);
        }

        System.out.println("[BuckyBucketLister] Found " + allFiles.size() + " files for " + dayPrefix);
        return allFiles;
    }

    @Override
    public List<String> listDayFileNames(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        final String bucketPrefix = current().bucketPathPrefix();

        final List<String> allNames = new ArrayList<>();
        int failedNodes = 0;

        for (int nodeId = minNodeAccountId; nodeId <= maxNodeAccountId; nodeId++) {
            try {
                String recordPrefix = bucketPrefix + "record0.0." + nodeId + "/" + dayPrefix;
                allNames.addAll(listObjectNamesWithPrefix(recordPrefix));

                String sidecarPrefix = bucketPrefix + "record0.0." + nodeId + "/sidecar/" + dayPrefix;
                allNames.addAll(listObjectNamesWithPrefix(sidecarPrefix));
            } catch (Exception e) {
                failedNodes++;
                System.err.println(
                        "[BuckyBucketLister] Failed to list file names for node " + nodeId + ": " + e.getMessage());
            }
        }

        // Fail if all nodes failed to list
        int totalNodes = maxNodeAccountId - minNodeAccountId + 1;
        if (failedNodes == totalNodes) {
            throw new RuntimeException("Failed to list file names for all nodes on day: " + dayPrefix);
        }

        return allNames;
    }

    private List<ChainFile> listObjectsWithPrefix(String prefix, int nodeAccountId) {
        final List<ChainFile> files = new ArrayList<>();
        final List<String> keys = listObjectKeysWithPrefix(prefix);

        for (String key : keys) {
            try {
                // NOTE: Bucky's listObjectsPage only returns keys, not metadata (size, etag)
                // We'll create ChainFile with size=0 and etag=null as placeholders
                // This is a limitation of the Bucky library
                // TODO: Consider using AWS S3 SDK which returns full metadata
                ChainFile cf = ChainFile.createOrNull(nodeAccountId, key, 0, null);
                if (cf != null) {
                    files.add(cf);
                }
            } catch (Exception e) {
                System.err.println("[BuckyBucketLister] Failed to process object: " + e.getMessage());
            }
        }

        return files;
    }

    private List<String> listObjectNamesWithPrefix(String prefix) {
        return listObjectKeysWithPrefix(prefix);
    }

    /**
     * Performs paginated S3 object listing with the given prefix.
     *
     * @param prefix the S3 key prefix to filter objects
     * @return list of all object keys matching the prefix
     * @throws RuntimeException if the S3 listing operation fails
     */
    private List<String> listObjectKeysWithPrefix(String prefix) {
        final List<String> keys = new ArrayList<>();

        try {
            // Bucky's listObjectsPage: (prefix, continuationToken, delimiter, maxResults)
            String token = null;
            do {
                S3Client.ListPage page = s3Client.listObjectsPage(prefix, token, null, S3Client.LIST_OBJECTS_MAX);
                keys.addAll(page.keys());
                token = page.continuationToken();
            } while (token != null);

        } catch (Exception e) {
            throw new RuntimeException("Failed to list objects with prefix " + prefix + ": " + e.getMessage(), e);
        }

        return keys;
    }
}
