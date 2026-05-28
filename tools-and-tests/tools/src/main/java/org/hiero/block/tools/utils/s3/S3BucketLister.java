// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.s3;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.records.ChainFile;
import org.hiero.block.tools.records.RecordFileDates;
import org.hiero.block.tools.utils.BucketLister;

/**
 * S3-based implementation of {@link BucketLister} using MinIO S3 client.
 *
 * <p>Provides file listing capabilities for S3-compatible storage (AWS S3, MinIO, etc.)
 * with optional caching support for improved performance.
 */
public final class S3BucketLister implements BucketLister {

    private final MinioClient minioClient;
    private final String bucketName;
    private final boolean cacheEnabled;
    private final Path cacheDir;
    private final int minNodeAccountId;
    private final int maxNodeAccountId;

    /**
     * Creates a new S3 bucket lister.
     *
     * @param endpoint       S3 endpoint URL
     * @param region         S3 region
     * @param accessKey      S3 access key
     * @param secretKey      S3 secret key
     * @param bucketName     S3 bucket name
     * @param cacheEnabled   whether to enable file listing cache
     * @param cacheDir       directory for cache files
     * @param minNodeAccountId minimum node account ID
     * @param maxNodeAccountId maximum node account ID
     */
    public S3BucketLister(
            final String endpoint,
            final String region,
            final String accessKey,
            final String secretKey,
            final String bucketName,
            final boolean cacheEnabled,
            final Path cacheDir,
            final int minNodeAccountId,
            final int maxNodeAccountId) {
        this.bucketName = bucketName;
        this.cacheEnabled = cacheEnabled;
        this.cacheDir = cacheDir;
        this.minNodeAccountId = minNodeAccountId;
        this.maxNodeAccountId = maxNodeAccountId;
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .region(region)
                .build();
    }

    @Override
    public List<ChainFile> listDay(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the day (e.g., "2024-01-01")
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        return listWithFilePrefix(dayPrefix);
    }

    @Override
    public List<String> listDayFileNames(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the day
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        return listNamesWithFilePrefix(dayPrefix);
    }

    /**
     * Lists all ChainFiles with the given file prefix.
     *
     * @param filePrefix the file name prefix (e.g., "2024-01-01")
     * @return list of ChainFile objects
     */
    private List<ChainFile> listWithFilePrefix(String filePrefix) {
        try {
            // Check cache first
            final Path listCacheFilePath = cacheDir.resolve("list-s3-" + filePrefix + ".bin.gz");
            if (cacheEnabled && Files.exists(listCacheFilePath)) {
                try (ObjectInputStream ois =
                        new ObjectInputStream(new GZIPInputStream(Files.newInputStream(listCacheFilePath)))) {
                    final int fileCount = ois.readInt();
                    final List<ChainFile> chainFiles = new ArrayList<>(fileCount);
                    for (int i = 0; i < fileCount; i++) {
                        chainFiles.add((ChainFile) ois.readObject());
                    }
                    return chainFiles;
                }
            }

            // List files from S3 for all nodes
            List<ChainFile> chainFiles = IntStream.range(minNodeAccountId, maxNodeAccountId + 1)
                    .parallel()
                    .mapToObj(nodeAccountId -> {
                        try {
                            // List record files
                            String recordPrefix = "recordstreams/record0.0." + nodeAccountId + "/" + filePrefix;
                            Stream<ChainFile> recordFiles = listObjects(recordPrefix, nodeAccountId);

                            // List sidecar files
                            String sidecarPrefix =
                                    "recordstreams/record0.0." + nodeAccountId + "/sidecar/" + filePrefix;
                            Stream<ChainFile> sidecarFiles = listObjects(sidecarPrefix, nodeAccountId);

                            return Stream.concat(recordFiles, sidecarFiles);
                        } catch (Exception e) {
                            System.err.println(
                                    "[WARN] Failed to list files for node " + nodeAccountId + ": " + e.getMessage());
                            return Stream.<ChainFile>empty();
                        }
                    })
                    .flatMap(Function.identity())
                    .toList();

            // Cache the results
            if (cacheEnabled) {
                Files.createDirectories(listCacheFilePath.getParent());
                try (ObjectOutputStream oos =
                        new ObjectOutputStream(new GZIPOutputStream(Files.newOutputStream(listCacheFilePath)))) {
                    oos.writeInt(chainFiles.size());
                    for (ChainFile chainFile : chainFiles) {
                        oos.writeObject(chainFile);
                    }
                }
            }

            return chainFiles;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list files with prefix: " + filePrefix, e);
        }
    }

    /**
     * Lists all file names with the given file prefix.
     *
     * @param filePrefix the file name prefix
     * @return list of file paths
     */
    private List<String> listNamesWithFilePrefix(String filePrefix) {
        try {
            // Check cache first
            final Path listCacheFilePath = cacheDir.resolve("list-names-s3-" + filePrefix + ".txt.gz");
            if (cacheEnabled && Files.exists(listCacheFilePath)) {
                try (var lineStream = Files.lines(listCacheFilePath)) {
                    return lineStream.toList();
                }
            }

            // List files from S3
            List<String> fileNames = IntStream.range(minNodeAccountId, maxNodeAccountId + 1)
                    .parallel()
                    .mapToObj(nodeAccountId -> {
                        try {
                            String recordPrefix = "recordstreams/record0.0." + nodeAccountId + "/" + filePrefix;
                            String sidecarPrefix =
                                    "recordstreams/record0.0." + nodeAccountId + "/sidecar/" + filePrefix;

                            Stream<String> recordNames = listObjectNames(recordPrefix);
                            Stream<String> sidecarNames = listObjectNames(sidecarPrefix);

                            return Stream.concat(recordNames, sidecarNames);
                        } catch (Exception e) {
                            System.err.println("[WARN] Failed to list file names for node " + nodeAccountId + ": "
                                    + e.getMessage());
                            return Stream.<String>empty();
                        }
                    })
                    .flatMap(Function.identity())
                    .toList();

            // Cache the results
            if (cacheEnabled) {
                Files.createDirectories(listCacheFilePath.getParent());
                Files.write(listCacheFilePath, fileNames);
            }

            return fileNames;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list file names with prefix: " + filePrefix, e);
        }
    }

    /**
     * Lists S3 objects with the given prefix and converts them to ChainFiles.
     *
     * @param prefix        the S3 object prefix
     * @param nodeAccountId the node account ID
     * @return stream of ChainFile objects
     */
    private Stream<ChainFile> listObjects(String prefix, int nodeAccountId) {
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .recursive(true)
                    .build());

            List<ChainFile> files = new ArrayList<>();
            for (Result<Item> result : results) {
                try {
                    Item item = result.get();
                    String objectName = item.objectName();
                    long size = item.size();
                    String etag = item.etag(); // S3 ETag (hex-encoded MD5)
                    String md5Base64 = convertETagToBase64Md5(etag); // Convert to base64 (GCS format)

                    ChainFile cf = ChainFile.createOrNull(nodeAccountId, objectName, (int) size, md5Base64);
                    if (cf == null) {
                        System.err.println("[WARN] Skipping unrecognized S3 file: " + objectName);
                    } else {
                        files.add(cf);
                    }
                } catch (Exception e) {
                    System.err.println("[WARN] Failed to process S3 object: " + e.getMessage());
                }
            }
            // Debug: Count signature files
            long sigCount = files.stream()
                    .filter(f -> f.path().endsWith(".rcd_sig") || f.path().endsWith(".rcd_sig.gz"))
                    .count();
            long rcdCount = files.stream()
                    .filter(f -> f.path().endsWith(".rcd") || f.path().endsWith(".rcd.gz"))
                    .count();
            if (sigCount > 0 || rcdCount > 0) {
                System.out.println("[S3BucketLister] Found " + rcdCount + " .rcd files and " + sigCount
                        + " .rcd_sig files for prefix: " + prefix);
            }
            return files.stream().filter(Objects::nonNull);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list objects with prefix: " + prefix, e);
        }
    }

    /**
     * Lists S3 object names with the given prefix.
     *
     * @param prefix the S3 object prefix
     * @return stream of object names
     */
    private Stream<String> listObjectNames(String prefix) {
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .recursive(true)
                    .build());

            List<String> names = new ArrayList<>();
            for (Result<Item> result : results) {
                try {
                    Item item = result.get();
                    names.add(item.objectName());
                } catch (Exception e) {
                    System.err.println("[WARN] Failed to get object name: " + e.getMessage());
                }
            }
            return names.stream();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list object names with prefix: " + prefix, e);
        }
    }

    /**
     * Converts S3 ETag to base64-encoded MD5 format (to match GCS format).
     *
     * <p>S3 ETags are typically hex-encoded MD5 hashes (with optional quotes).
     * For multipart uploads, ETags have format "hex-partcount" and are not valid MD5s.
     * This method converts simple ETags from hex to base64 to match the format
     * expected by UpdateDayListingsCommand (which expects GCS-style base64 MD5s).
     *
     * @param etag the S3 ETag (hex format, possibly quoted)
     * @return base64-encoded MD5, or original etag if conversion fails
     */
    private static String convertETagToBase64Md5(String etag) {
        if (etag == null || etag.isEmpty()) {
            return etag;
        }

        // Remove quotes if present
        String cleaned = etag.replace("\"", "");

        // Skip multipart upload ETags (format: "hex-partcount")
        if (cleaned.contains("-")) {
            // For multipart uploads, return a placeholder base64 string
            // Since we can't get the real MD5, we'll use the ETag itself
            // This is okay because we're not using MD5 for validation in Solo
            return Base64.getEncoder().encodeToString(cleaned.getBytes());
        }

        // Convert hex to base64
        try {
            // Parse hex string to bytes
            byte[] md5Bytes = HexFormat.of().parseHex(cleaned);
            // Encode to base64
            return Base64.getEncoder().encodeToString(md5Bytes);
        } catch (Exception e) {
            System.err.println("[WARN] Failed to convert ETag to base64: " + etag + " - " + e.getMessage());
            // Return a base64-encoded version of the original string as fallback
            return Base64.getEncoder().encodeToString(cleaned.getBytes());
        }
    }
}
