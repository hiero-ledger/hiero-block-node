// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.gcp;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Utility class to list and download account balance protobuf files from the mainnet bucket.
 * The balance files are located at: gs://hedera-mainnet-streams/accountBalances/balance{nodeAccountId}/
 *
 * <p>Example bucket paths:
 * <ul>
 *   <li>{@code gs://hedera-mainnet-streams/accountBalances/balance0.0.3/2024-01-15T00_00_00.000000Z_Balances.pb.gz}</li>
 *   <li>{@code gs://hedera-mainnet-streams/accountBalances/balance0.0.3/2024-01-15T00_00_00.000000Z_Balances.pb.sig.gz}</li>
 * </ul>
 */
public class BalanceFileBucket {

    /** The mainnet bucket name */
    private static final String HEDERA_MAINNET_STREAMS_BUCKET = "hedera-mainnet-streams";

    /** Blob name field only */
    private static final BlobListOption NAME_FIELD_ONLY = BlobListOption.fields(BlobField.NAME);

    /** The GCP Storage service instance */
    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();

    /** Maximum number of retry attempts for GCP operations */
    private static final int MAX_RETRIES = 3;

    /** Initial delay in milliseconds between retries */
    private static final long INITIAL_RETRY_DELAY_MS = 1000;

    /** Formatter for balance file timestamps with 9-digit nanoseconds (e.g., 2019-09-13T22_00_00.000081000Z) */
    private static final DateTimeFormatter BALANCE_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss.SSSSSSSSS'Z'").withZone(ZoneOffset.UTC);

    /** Formatter for balance file timestamps with 6-digit nanoseconds (e.g., 2019-09-13T22_00_00.000081Z) */
    private static final DateTimeFormatter BALANCE_TIMESTAMP_FORMATTER_6 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

    /** The cache enabled switch */
    private final boolean cacheEnabled;

    /** The cache directory */
    private final Path cacheDir;

    /** The minimum node account id in the network */
    private final int minNodeAccountId;

    /** The maximum node account id in the network */
    private final int maxNodeAccountId;

    /** The GCP project to bill for requester-pays bucket access */
    private final String userProject;

    /**
     * Create a new BalanceFileBucket instance.
     *
     * @param cacheEnabled whether to cache downloaded files
     * @param cacheDir the cache directory
     * @param minNodeAccountId the minimum node account id
     * @param maxNodeAccountId the maximum node account id
     * @param userProject the GCP project for requester-pays (can be null)
     */
    public BalanceFileBucket(
            boolean cacheEnabled, Path cacheDir, int minNodeAccountId, int maxNodeAccountId, String userProject) {
        this.cacheEnabled = cacheEnabled;
        this.cacheDir = cacheDir;
        this.minNodeAccountId = minNodeAccountId;
        this.maxNodeAccountId = maxNodeAccountId;
        this.userProject = userProject;
    }

    /**
     * List all balance file timestamps available for a given day.
     *
     * @param dayPrefix the day prefix in format "YYYY-MM-DD" (e.g., "2019-09-13")
     * @return list of Instant timestamps for available balance files
     */
    public List<Instant> listBalanceTimestampsForDay(String dayPrefix) {
        List<Instant> timestamps = new ArrayList<>();
        String prefix = "accountBalances/balance0.0." + minNodeAccountId + "/" + dayPrefix;

        try {
            BlobListOption[] options = userProject != null
                    ? new BlobListOption[] {
                        BlobListOption.prefix(prefix), NAME_FIELD_ONLY, BlobListOption.userProject(userProject)
                    }
                    : new BlobListOption[] {BlobListOption.prefix(prefix), NAME_FIELD_ONLY};

            STORAGE.list(HEDERA_MAINNET_STREAMS_BUCKET, options)
                    .streamAll()
                    .map(BlobInfo::getName)
                    .filter(name -> name.endsWith("_Balances.pb.gz") || name.endsWith("_Balances.pb"))
                    .forEach(name -> {
                        try {
                            String filename = name.substring(name.lastIndexOf('/') + 1);
                            String timestampStr =
                                    filename.replace("_Balances.pb.gz", "").replace("_Balances.pb", "");
                            // Parse the timestamp - handle variable nanosecond precision
                            String normalized = normalizeTimestamp(timestampStr);
                            Instant instant = Instant.parse(normalized);
                            timestamps.add(instant);
                        } catch (Exception e) {
                            System.err.println("Warning: Could not parse timestamp from: " + name);
                        }
                    });
        } catch (Exception e) {
            System.err.println("Warning: Error listing balance files for day " + dayPrefix + ": " + e.getMessage());
        }

        return timestamps;
    }

    /**
     * Normalize a timestamp string with underscores to ISO-8601 format.
     * Handles variable nanosecond precision (6 or 9 digits).
     * Package-private for testing.
     *
     * @param timestampStr timestamp like "2019-09-13T22_00_00.000081Z"
     * @return ISO-8601 format like "2019-09-13T22:00:00.000081Z"
     */
    String normalizeTimestamp(String timestampStr) {
        // Replace underscores with colons for ISO-8601 format
        String normalized = timestampStr.replace('_', ':');
        // Ensure nanoseconds are 9 digits for proper parsing
        int dotIndex = normalized.indexOf('.');
        int zIndex = normalized.indexOf('Z');
        if (dotIndex > 0 && zIndex > dotIndex) {
            String nanos = normalized.substring(dotIndex + 1, zIndex);
            if (nanos.length() < 9) {
                nanos = nanos + "0".repeat(9 - nanos.length());
                normalized = normalized.substring(0, dotIndex + 1) + nanos + "Z";
            }
        }
        return normalized;
    }

    /**
     * Get the midnight timestamp for a given day in format suitable for balance file lookup.
     * Midnight balance files are generated daily at 00:00:00 UTC.
     *
     * @param dayPrefix the day in format "YYYY-MM-DD" (e.g., "2024-01-15")
     * @return the midnight Instant for that day
     */
    public static Instant getMidnightTimestamp(String dayPrefix) {
        return Instant.parse(dayPrefix + "T00:00:00Z");
    }

    /**
     * Download the midnight balance file for a given day directly without listing.
     * This is more efficient than listing all files for a day when only the midnight
     * checkpoint is needed.
     *
     * @param dayPrefix the day in format "YYYY-MM-DD" (e.g., "2024-01-15")
     * @return the protobuf file bytes (decompressed if gzipped), or null if not found
     */
    public byte[] downloadMidnightBalanceFile(String dayPrefix) {
        Instant midnight = getMidnightTimestamp(dayPrefix);
        return downloadBalanceFile(midnight);
    }

    /**
     * Download a balance protobuf file for the given timestamp from any available node.
     *
     * @param timestamp the timestamp of the balance file
     * @return the protobuf file bytes (decompressed if gzipped), or null if not found
     */
    public byte[] downloadBalanceFile(Instant timestamp) {
        // Try multiple timestamp formats (6-digit and 9-digit nanoseconds) due to varying precision in GCP
        String[] timestampFormats = {
            formatTimestamp6(timestamp), // 6-digit nanoseconds (most common)
            formatTimestamp(timestamp) // 9-digit nanoseconds
        };

        // Try each node until we find one with the file
        for (int nodeId = minNodeAccountId; nodeId <= maxNodeAccountId; nodeId++) {
            // Try gzipped first, then plain, with both timestamp formats
            for (String timestampStr : timestampFormats) {
                for (String suffix : new String[] {"_Balances.pb.gz", "_Balances.pb"}) {
                    String path = "accountBalances/balance0.0." + nodeId + "/" + timestampStr + suffix;
                    try {
                        return download(path);
                    } catch (Exception ignored) {
                        // Try next format/node
                    }
                }
            }
        }
        return null;
    }

    /**
     * Download a balance signature file for the given timestamp and node.
     *
     * @param timestamp the timestamp of the balance file
     * @param nodeAccountId the node account ID (e.g., 3 for 0.0.3)
     * @return the signature file bytes, or null if not found
     */
    public byte[] downloadBalanceSignatureFile(Instant timestamp, int nodeAccountId) {
        String timestampStr = formatTimestamp(timestamp);
        String path = "accountBalances/balance0.0." + nodeAccountId + "/" + timestampStr + "_Balances.pb.sig.gz";
        try {
            return download(path);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Format an Instant to the balance file timestamp format with 9-digit nanoseconds.
     * Package-private for testing.
     *
     * @param timestamp the timestamp to format
     * @return formatted string like "2019-09-13T22_00_00.000081000Z"
     */
    String formatTimestamp(Instant timestamp) {
        return BALANCE_TIMESTAMP_FORMATTER.format(timestamp);
    }

    /**
     * Format an Instant to the balance file timestamp format with 6-digit nanoseconds.
     * Many GCP balance files use 6-digit (microsecond) precision.
     * Package-private for testing.
     *
     * @param timestamp the timestamp to format
     * @return formatted string like "2019-09-13T22_00_00.000081Z"
     */
    String formatTimestamp6(Instant timestamp) {
        return BALANCE_TIMESTAMP_FORMATTER_6.format(timestamp);
    }

    /**
     * Download a file from GCP, caching if enabled.
     *
     * @param path the path to the file in the bucket
     * @return the bytes of the file (decompressed if gzipped)
     */
    private byte[] download(String path) {
        try {
            Path cachedFilePath = cacheDir.resolve(path);
            byte[] rawBytes;
            if (cacheEnabled && Files.exists(cachedFilePath)) {
                rawBytes = Files.readAllBytes(cachedFilePath);
            } else {
                rawBytes = downloadWithRetry(path);
                if (cacheEnabled) {
                    Files.createDirectories(cachedFilePath.getParent());
                    Path tempFile = Files.createTempFile(cacheDir, null, ".tmp");
                    Files.write(tempFile, rawBytes);
                    Files.move(tempFile, cachedFilePath);
                }
            }
            // Decompress if gzipped
            if (path.endsWith(".gz")) {
                try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(rawBytes))) {
                    return gzipIn.readAllBytes();
                }
            }
            return rawBytes;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to download: " + path, e);
        }
    }

    /**
     * Download with retry and exponential backoff.
     */
    private byte[] downloadWithRetry(String path) {
        long delay = INITIAL_RETRY_DELAY_MS;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            var blobOptions = userProject != null
                    ? new BlobGetOption[] {BlobGetOption.userProject(userProject)}
                    : new BlobGetOption[0];
            var blob = STORAGE.get(BlobId.of(HEDERA_MAINNET_STREAMS_BUCKET, path), blobOptions);
            if (blob != null) {
                return userProject != null
                        ? blob.getContent(com.google.cloud.storage.Blob.BlobSourceOption.userProject(userProject))
                        : blob.getContent();
            }
            if (attempt < MAX_RETRIES) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while downloading: " + path, e);
                }
                delay *= 2;
            }
        }
        throw new IllegalStateException("Blob not found after " + MAX_RETRIES + " attempts: " + path);
    }
}
