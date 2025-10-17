// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks.gcp;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
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
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.commands.record2blocks.model.ChainFile;
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
public class MainNetBucket {
    /** The required fields we need from blobs */
    private static final Storage.BlobListOption REQUIRED_FIELDS =
            BlobListOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.MD5HASH);
    /** Blob name field only */
    private static final Storage.BlobListOption NAME_FIELD_ONLY = BlobListOption.fields(BlobField.NAME);
    /** The mainnet bucket name*/
    private static final String HEDERA_MAINNET_STREAMS_BUCKET = "hedera-mainnet-streams";
    /** The mainnet bucket GCP API instance */
    private static final Bucket STREAMS_BUCKET =
            StorageOptions.getDefaultInstance().getService().get(HEDERA_MAINNET_STREAMS_BUCKET);

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
    /**
     * Create a new MainNetBucket instance with the given cache enabled switch and cache directory.
     *
     * @param cacheEnabled the cache enabled switch
     * @param cacheDir the cache directory
     * @param minNodeAccountId the minimum node account id in the network
     * @param maxNodeAccountId the maximum node account id in the network
     */
    public MainNetBucket(boolean cacheEnabled, Path cacheDir, int minNodeAccountId, int maxNodeAccountId) {
        this.cacheEnabled = cacheEnabled;
        this.cacheDir = cacheDir;
        this.minNodeAccountId = minNodeAccountId;
        this.maxNodeAccountId = maxNodeAccountId;
    }

    /**
     * Download a file from GCP, caching if CACHE_ENABLED is true. This is designed to be thread safe.
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
                rawBytes = STREAMS_BUCKET.get(path).getContent();
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
     * Download a file from GCP as a stream, caching if CACHE_ENABLED is true. This is designed to be thread safe.
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
                final byte[] bytes = STREAMS_BUCKET.get(path).getContent();
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
                                    STREAMS_BUCKET
                                            .list(
                                                    BlobListOption.prefix("recordstreams/record0.0." + nodeAccountId
                                                            + "/" + filePrefix),
                                                    REQUIRED_FIELDS)
                                            .streamAll(),
                                    STREAMS_BUCKET
                                            .list(
                                                    BlobListOption.prefix("recordstreams/record0.0." + nodeAccountId
                                                            + "/sidecar/" + filePrefix),
                                                    REQUIRED_FIELDS)
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
                    .mapToObj(nodeAccountId -> STREAMS_BUCKET
                            .list(
                                    BlobListOption.prefix(
                                            "recordstreams/record0.0." + nodeAccountId + "/" + filePrefix),
                                    NAME_FIELD_ONLY)
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
