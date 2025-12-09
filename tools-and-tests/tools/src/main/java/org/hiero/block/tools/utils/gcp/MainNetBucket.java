// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.gcp;

import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;

import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.records.ChainFile;
import org.hiero.block.tools.records.RecordFileDates;

/**
 * A class to list and download files from the mainnet bucket. This is designed to be thread safe.
 * <p>
 * <b>Example bucket paths</b>
 * <ul>
 *    <li><code>gs://hedera-mainnet-streams/recordstreams/record0.0.3/2019-09-13T21_53_51.396440Z.rcd</code></li>
 *    <li><code>gs://hedera-mainnet-streams/recordstreams/record0.0.3/sidecar/2023-04-25T17_42_16.032498578Z_01.rcd.gz</code></li>
 * </ul>
 */
@SuppressWarnings("unused")
public class MainNetBucket {
    /** The required fields we need from blobs */
    private static final Storage.BlobListOption REQUIRED_FIELDS =
            BlobListOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.MD5HASH);
    /** Blob name field only */
    private static final Storage.BlobListOption NAME_FIELD_ONLY = BlobListOption.fields(BlobField.NAME);
    /** Glob filter to signature files only */
    private static final Storage.BlobListOption SIGNATURE_FILES_ONLY = BlobListOption.matchGlob("**.rcd_sig");
    /** The mainnet bucket name*/
    private static final String HEDERA_MAINNET_STREAMS_BUCKET = "hedera-mainnet-streams";
    /** The GCP Storage service instance - use Storage.list() directly to avoid needing bucket metadata access */
    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();

    /**
     * The cache enabled switch. When caching is enabled all fetched data is saved on disk and reused between runs. This
     * is useful for debugging and development. But when we get to doing long runs with many TB of data this is not
     * practical.
     */
    private final boolean cacheEnabled;

    /** The cache directory, where we store all downloaded content for reuse if CACHE_ENABLED is true. */
    private final Path cacheDir; // = DATA_DIR.resolve("gcp-cache");

    /** The minimum node account id in the network. */
    private final int minNodeAccountId;

    /** The maximum node account id in the network. */
    private final int maxNodeAccountId;

    /** The GCP project to bill for requester-pays bucket access. */
    private final String userProject;

    /**
     * Create a new MainNetBucket instance with the given cache enabled switch and cache directory.
     *
     * @param cacheEnabled the cache enabled switch
     * @param cacheDir the cache directory
     * @param minNodeAccountId the minimum node account id in the network
     * @param maxNodeAccountId the maximum node account id in the network
     */
    public MainNetBucket(boolean cacheEnabled, Path cacheDir, int minNodeAccountId, int maxNodeAccountId) {
        this(cacheEnabled, cacheDir, minNodeAccountId, maxNodeAccountId, null);
    }

    /**
     * Create a new MainNetBucket instance with the given cache enabled switch, cache directory, and user project.
     *
     * @param cacheEnabled the cache enabled switch
     * @param cacheDir the cache directory
     * @param minNodeAccountId the minimum node account id in the network
     * @param maxNodeAccountId the maximum node account id in the network
     * @param userProject the GCP project to bill for requester-pays bucket access (can be null)
     */
    public MainNetBucket(
            boolean cacheEnabled, Path cacheDir, int minNodeAccountId, int maxNodeAccountId, String userProject) {
        this.cacheEnabled = cacheEnabled;
        this.cacheDir = cacheDir;
        this.minNodeAccountId = minNodeAccountId;
        this.maxNodeAccountId = maxNodeAccountId;
        this.userProject = userProject;
    }

    /** Maximum number of retry attempts for GCP operations. */
    private static final int MAX_RETRIES = 3;

    /** Initial delay in milliseconds between retries (will be doubled for each retry). */
    private static final long INITIAL_RETRY_DELAY_MS = 1000;

    /**
     * Download a file from GCP, caching if CACHE_ENABLED is true. This is designed to be thread safe.
     * Retries up to MAX_RETRIES times with exponential backoff if the blob is not found.
     *
     * @param path the path to the file in the bucket
     * @return the bytes of the file
     */
    public byte[] download(String path) {
        try {
            final Path cachedFilePath = cacheDir.resolve(path);
            byte[] rawBytes;
            if (cacheEnabled && Files.exists(cachedFilePath)) {
                rawBytes = Files.readAllBytes(cachedFilePath);
            } else {
                rawBytes = downloadWithRetry(path);
                if (cacheEnabled) {
                    Files.createDirectories(cachedFilePath.getParent());
                    Path tempCachedFilePath = Files.createTempFile(cacheDir, null, ".tmp");
                    Files.write(tempCachedFilePath, rawBytes);
                    Files.move(tempCachedFilePath, cachedFilePath);
                }
            }
            // if file is gzipped, unzip it
            if (path.endsWith(".gz")) {
                try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(rawBytes))) {
                    return gzipInputStream.readAllBytes();
                }
            } else {
                return rawBytes;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Downloads a file from GCP with retry logic and exponential backoff.
     *
     * @param path the path to the file in the bucket
     * @return the bytes of the file
     * @throws RuntimeException if the file cannot be downloaded after all retries
     */
    private byte[] downloadWithRetry(String path) {
        long delay = INITIAL_RETRY_DELAY_MS;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            var blob =
                    STORAGE.get(BlobId.of(HEDERA_MAINNET_STREAMS_BUCKET, path), BlobGetOption.userProject(userProject));
            if (blob != null) {
                return blob.getContent(BlobSourceOption.userProject(userProject));
            }
            if (attempt < MAX_RETRIES) {
                System.err.println("Warning: Blob not found for path '" + path + "', attempt " + attempt + " of "
                        + MAX_RETRIES + ". Retrying in " + delay + "ms...");
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry download for: " + path, e);
                }
                delay *= 2; // Exponential backoff
            }
        }
        throw new RuntimeException("Blob not found after " + MAX_RETRIES + " attempts for path: " + path);
    }

    /**
     * Download a file from GCP as a stream, caching if CACHE_ENABLED is true. This is designed to be thread safe.
     * Retries up to MAX_RETRIES times with exponential backoff if the blob is not found.
     *
     * @param path the path to the file in the bucket
     * @return the stream of the file
     */
    public java.io.InputStream downloadStreaming(String path) {
        try {
            Path cachedFilePath = cacheDir.resolve(path);
            if (cacheEnabled && Files.exists(cachedFilePath)) {
                return Files.newInputStream(cachedFilePath, StandardOpenOption.READ);
            } else {
                final byte[] bytes = downloadWithRetryNoUserProject(path);
                if (cacheEnabled) {
                    Files.createDirectories(cachedFilePath.getParent());
                    Path tempCachedFilePath = Files.createTempFile(cacheDir, null, ".tmp");
                    Files.write(tempCachedFilePath, bytes);
                    Files.move(tempCachedFilePath, cachedFilePath);
                }
                return new ByteArrayInputStream(bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Downloads a file from GCP with retry logic and exponential backoff (without userProject option).
     *
     * @param path the path to the file in the bucket
     * @return the bytes of the file
     * @throws RuntimeException if the file cannot be downloaded after all retries
     */
    private byte[] downloadWithRetryNoUserProject(String path) {
        long delay = INITIAL_RETRY_DELAY_MS;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            var blob = STORAGE.get(BlobId.of(HEDERA_MAINNET_STREAMS_BUCKET, path));
            if (blob != null) {
                return blob.getContent();
            }
            if (attempt < MAX_RETRIES) {
                System.err.println("Warning: Blob not found for path '" + path + "', attempt " + attempt + " of "
                        + MAX_RETRIES + ". Retrying in " + delay + "ms...");
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry download for: " + path, e);
                }
                delay *= 2; // Exponential backoff
            }
        }
        throw new RuntimeException("Blob not found after " + MAX_RETRIES + " attempts for path: " + path);
    }

    /**
     * List all the ChainFiles in the bucket that have a start time within the given day that contains the given
     * blockStartTime. This fetches blobs for all record files, signature files and sidecar files. For all nodes.
     *
     * @param blockStartTime the start time of a block, in nanoseconds since OA
     * @return a stream of ChainFiles that contain the records for the given day.
     */
    @SuppressWarnings("unused")
    public List<ChainFile> listDay(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the hour
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        return listWithFilePrefix(dayPrefix);
    }

    /**
     * List all the record file names in the bucket that have a start time within the given day that contains the given
     * blockStartTime.
     *
     * @param blockStartTime the start time of a block, in nanoseconds since OA
     * @return a list of unique names of all record files starting with the given file name prefix.
     */
    public List<String> listDayFileNames(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the hour
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        return listNamesWithFilePrefix(dayPrefix);
    }

    /**
     * List all the ChainFiles in the bucket that have a start time within the given hour that contains the given
     * blockStartTime. This fetches blobs for all record files, signature files and sidecar files. For all nodes.
     *
     * @param blockStartTime the start time of a block, in nanoseconds since OA
     * @return a stream of ChainFiles that contain the records for the given hour.
     */
    public List<ChainFile> listHour(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the hour
        final String hourPrefix = datePrefix.substring(0, datePrefix.indexOf('_'));
        return listWithFilePrefix(hourPrefix);
    }

    /**
     * Creates an array of BlobListOptions including the prefix, fields, and optionally userProject.
     *
     * @param prefix the prefix to filter blobs
     * @param fieldsOption the fields option to include
     * @return array of BlobListOptions
     */
    private BlobListOption[] getBlobListOptions(String prefix, BlobListOption fieldsOption) {
        if (userProject != null) {
            return new BlobListOption[] {
                BlobListOption.prefix(prefix), fieldsOption, BlobListOption.userProject(userProject)
            };
        } else {
            return new BlobListOption[] {BlobListOption.prefix(prefix), fieldsOption};
        }
    }

    /**
     * List all the ChainFiles in the bucket that have this file name prefix. This fetches blobs for all record files,
     * signature files and sidecar files. For all nodes.
     *
     * @param filePrefix the prefix of the file name to search for
     * @return a stream of ChainFiles that have a filename that starts with the given prefix.
     */
    private List<ChainFile> listWithFilePrefix(String filePrefix) {
        try {
            // read from cache if it already exists in cache
            final Path listCacheFilePath = cacheDir.resolve("list-" + filePrefix + ".bin.gz");
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
            // create a list of ChainFiles
            List<ChainFile> chainFiles = IntStream.range(minNodeAccountId, maxNodeAccountId + 1)
                    .parallel()
                    .mapToObj(nodeAccountId -> Stream.concat(
                                    STORAGE.list(
                                                    HEDERA_MAINNET_STREAMS_BUCKET,
                                                    getBlobListOptions(
                                                            "recordstreams/record0.0." + nodeAccountId + "/"
                                                                    + filePrefix,
                                                            REQUIRED_FIELDS))
                                            .streamAll(),
                                    STORAGE.list(
                                                    HEDERA_MAINNET_STREAMS_BUCKET,
                                                    getBlobListOptions(
                                                            "recordstreams/record0.0." + nodeAccountId + "/sidecar/"
                                                                    + filePrefix,
                                                            REQUIRED_FIELDS))
                                            .streamAll())
                            .map(blob -> new ChainFile(
                                    nodeAccountId,
                                    blob.getName(),
                                    blob.getSize().intValue(),
                                    blob.getMd5())))
                    .flatMap(Function.identity())
                    .toList();
            // save the list to cache
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
            // return all the streams combined
            return chainFiles;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * List all the record file names in the bucket that have this file name prefix. This fetches blobs for all record files,
     * signature files and sidecar files. For all nodes.
     *
     * @param filePrefix the prefix of the file name to search for
     * @return a list of unique names of all record files starting with the given file name prefix.
     */
    private List<String> listNamesWithFilePrefix(String filePrefix) {
        try {
            // read from cache if it already exists in cache
            final Path listCacheFilePath = cacheDir.resolve("list-names-" + filePrefix + ".txt.gz");
            if (cacheEnabled && Files.exists(listCacheFilePath)) {
                try (var lineStream = Files.lines(listCacheFilePath)) {
                    return lineStream.toList();
                }
            }
            // create a list of ChainFiles
            List<String> fileNames = IntStream.range(minNodeAccountId, maxNodeAccountId + 1)
                    .parallel()
                    .mapToObj(nodeAccountId -> STORAGE.list(
                                    HEDERA_MAINNET_STREAMS_BUCKET,
                                    getBlobListOptions(
                                            "recordstreams/record0.0." + nodeAccountId + "/" + filePrefix,
                                            NAME_FIELD_ONLY))
                            .streamAll()
                            .map(BlobInfo::getName)
                            .map(name -> name.substring(name.lastIndexOf('/') + 1))
                            .filter(name -> name.endsWith(".rcd") || name.endsWith(".rcd.gz")))
                    .flatMap(Function.identity())
                    .sorted()
                    .distinct()
                    .toList();
            // save the list to cache
            if (cacheEnabled) {
                Files.createDirectories(listCacheFilePath.getParent());
                Files.write(listCacheFilePath, fileNames);
            }
            // return all the file names
            return fileNames;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check if a specific signature file exists in the bucket for a given node and block timestamp.
     * The signature file path format is: recordstreams/record{nodeAccountId}/{timestamp}.rcd_sig
     *
     * @param nodeAccountId the node account ID (e.g., "0.0.13")
     * @param blockTimestamp the block timestamp in bucket format (e.g., "2019-10-09T21_32_20.601587003Z")
     * @return true if the signature file exists in the bucket, false otherwise
     */
    public boolean signatureFileExists(String nodeAccountId, String blockTimestamp) {
        String path = "recordstreams/record" + nodeAccountId + "/" + blockTimestamp + ".rcd_sig";
        BlobId blobId = BlobId.of(HEDERA_MAINNET_STREAMS_BUCKET, path);
        try {
            var blob = STORAGE.get(blobId, Storage.BlobGetOption.fields(BlobField.NAME));
            return blob != null && blob.exists();
        } catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Check if multiple signature files exist in the bucket for given nodes and a block timestamp.
     * Returns a map of node account ID to whether the signature file exists.
     *
     * @param nodeAccountIds the set of node account IDs to check (e.g., "0.0.13", "0.0.14")
     * @param blockTimestamp the block timestamp in bucket format (e.g., "2019-10-09T21_32_20.601587003Z")
     * @return a map of node account ID to boolean indicating if signature exists in bucket
     */
    public Map<String, Boolean> checkSignatureFilesExist(Set<String> nodeAccountIds, String blockTimestamp) {
        Map<String, Boolean> results = new HashMap<>();
        for (String nodeAccountId : nodeAccountIds) {
            results.put(nodeAccountId, signatureFileExists(nodeAccountId, blockTimestamp));
        }
        return results;
    }

    /**
     * List all signature files in the bucket for a given day. Uses hour-based prefix filtering
     * for efficiency and filters to only .rcd_sig files.
     *
     * <p>Returns a map where keys are block timestamps (e.g., "2019-10-09T21_32_20.601587003Z")
     * and values are sets of node account IDs that have signature files for that block.
     *
     * @param dayPrefix the day prefix in format "YYYY-MM-DD" (e.g., "2019-10-09")
     * @return a map of block timestamp to set of node account IDs with signatures
     */
    public Map<Instant, Set<String>> listSignatureFilesForDay(String dayPrefix) {
        // Query each hour of the day (00-23) in parallel for efficiency
        return IntStream.range(0, 24)
                .parallel()
                .mapToObj(hour -> {
                    String hourPrefix = String.format("%sT%02d", dayPrefix, hour);
                    return listSignatureFilesWithPrefix(hourPrefix);
                })
                .collect(HashMap::new, Map::putAll, Map::putAll); // we know there are no overlapping keys
    }

    /**
     * List all signature files in the bucket with the given prefix.
     * Queries all nodes in parallel for efficiency.
     *
     * @param filePrefix the prefix to filter files (e.g., "2019-10-09T21" for hour 21)
     * @return a map of block timestamp to set of node account IDs with signatures
     */
    private Map<Instant, Set<String>> listSignatureFilesWithPrefix(String filePrefix) {
        return IntStream.range(minNodeAccountId, maxNodeAccountId + 1)
                .parallel()
                .mapToObj(nodeAccountId -> {
                    final String nodeAccountIdStr = "0.0." + nodeAccountId;
                    final String prefix = "recordstreams/record" + nodeAccountIdStr + "/" + filePrefix;

                    Map<Instant, Set<String>> nodeSignatures = new HashMap<>();
                    try {
                        STORAGE.list(HEDERA_MAINNET_STREAMS_BUCKET, new BlobListOption[] {
                                    BlobListOption.prefix(prefix),
                                    NAME_FIELD_ONLY,
                                    SIGNATURE_FILES_ONLY,
                                    BlobListOption.userProject(userProject)
                                })
                                .streamAll()
                                .map(BlobInfo::getName)
                                .filter(name -> name.endsWith(".rcd_sig"))
                                .forEach(name -> nodeSignatures
                                        .computeIfAbsent(extractRecordFileTime(name), k -> new HashSet<>())
                                        .add(nodeAccountIdStr));
                    } catch (Exception e) {
                        System.err.println("Warning: Error listing signatures for node " + nodeAccountIdStr
                                + " with prefix " + filePrefix + ": " + e.getMessage());
                    }
                    return nodeSignatures;
                })
                .collect(
                        HashMap::new,
                        (acc, map) -> map.forEach((k, v) -> acc.merge(k, v, (s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        })),
                        (map1, map2) -> map2.forEach((k, v) -> map1.merge(k, v, (s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        })));
    }

    /**
     * The main method test listing all the files in the bucket for the given day and hour.
     */
    public static void main(String[] args) {
        MainNetBucket mainNetBucket = new MainNetBucket(true, Path.of("data/gcp-cache"), 0, 34);
        mainNetBucket.listHour(0).forEach(System.out::println);
        System.out.println("==========================================================================");
        final Instant dec1st2024 = Instant.parse("2024-12-01T00:00:00Z");
        final long dec1st2024BlockTime = RecordFileDates.instantToBlockTimeLong(dec1st2024);
        mainNetBucket.listHour(dec1st2024BlockTime).forEach(System.out::println);
    }
}
