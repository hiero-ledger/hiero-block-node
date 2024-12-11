package com.hedera.block.tools.commands.record2blocks.gcp;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.hedera.block.tools.commands.record2blocks.model.ChainFile;
import com.hedera.block.tools.commands.record2blocks.util.RecordFileDates;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A class to list and download files from the mainnet bucket. This is designed to be thread safe.
 * <p>
 *    <b>Example bucket paths</b>
 *    <ul>
 *       <li><code>gs://hedera-mainnet-streams/recordstreams/record0.0.3/2019-09-13T21_53_51.396440Z.rcd</code></li>
 *       <li><code>gs://hedera-mainnet-streams/recordstreams/record0.0.3/sidecar/2023-04-25T17_42_16.032498578Z_01.rcd.gz</code></li>
 *    </ul>
 *    <ul><code></code></ul>
 * </p>
 */
public class MainNetBucket {
    /** The required fields we need from blobs */
    private static final Storage.BlobListOption REQUIRED_FIELDS =
            BlobListOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.MD5HASH);
    /** The mainnet bucket name*/
    private static final String HEDERA_MAINNET_STREAMS_BUCKET = "hedera-mainnet-streams";
    /** The mainnet bucket GCP API instance */
    private static final Bucket STREAMS_BUCKET = StorageOptions.getDefaultInstance().getService()
            .get(HEDERA_MAINNET_STREAMS_BUCKET);

    /**
     * The cache enabled switch. When caching is enabled all fetched data is saved on disk and reused between runs. This
     * is useful for debugging and development. But when we get to doing long runs with many TB of data this is not
     * practical.
     */
    private final boolean cacheEnabled;

    /** The cache directory, where we store all downloaded content for reuse if CACHE_ENABLED is true. */
    private final Path cacheDir;// = DATA_DIR.resolve("gcp-cache");

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
            Path cachedFilePath = cacheDir.resolve(path);
            if (cacheEnabled && Files.exists(cachedFilePath)) {
                return Files.readAllBytes(cachedFilePath);
            } else {
                byte[] bytes = STREAMS_BUCKET.get(path).getContent();
                if (cacheEnabled) {
                    Files.createDirectories(cachedFilePath.getParent());
                    Path tempCachedFilePath = Files.createTempFile(cacheDir, null, ".tmp");
                    Files.write(tempCachedFilePath, bytes);
                    Files.move(tempCachedFilePath, cachedFilePath);
                }
                return bytes;
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
    public List<ChainFile> listDay(long blockStartTime) {
        final String datePrefix = RecordFileDates.blockTimeLongToRecordFilePrefix(blockStartTime);
        // crop to the hour
        final String dayPrefix = datePrefix.substring(0, datePrefix.indexOf('T'));
        System.out.println("dayPrefix = " + dayPrefix);
        return listWithFilePrefix(dayPrefix);
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
            final Path listCacheFilePath = cacheDir.resolve("list-"+filePrefix+".bin.gz");
            if (cacheEnabled && Files.exists(listCacheFilePath)) {
                try(ObjectInputStream ois = new ObjectInputStream(
                        new GZIPInputStream(Files.newInputStream(listCacheFilePath)))) {
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
                    .mapToObj(nodeAccountId ->
                            Stream.concat(
                                            STREAMS_BUCKET.list(
                                                    BlobListOption.prefix("recordstreams/record0.0."+nodeAccountId+"/"+filePrefix),
                                                    REQUIRED_FIELDS
                                            ).streamAll(),
                                            STREAMS_BUCKET.list(
                                                    BlobListOption.prefix("recordstreams/record0.0."+nodeAccountId+"/sidecar/"+filePrefix),
                                                    REQUIRED_FIELDS
                                            ).streamAll()
                                    )
                                    .map(blob -> new ChainFile(nodeAccountId, blob.getName(),
                                            blob.getSize().intValue(), blob.getMd5()))
                    ).flatMap(Function.identity())
                    .toList();
            // save the list to cache
            if (cacheEnabled) {
                Files.createDirectories(listCacheFilePath.getParent());
                try(ObjectOutputStream oos = new ObjectOutputStream(
                        new GZIPOutputStream(Files.newOutputStream(listCacheFilePath)))) {
                    oos.writeInt(chainFiles.size());
                    for(ChainFile chainFile : chainFiles) {
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

//    /**
//     * Main method to test the listing of blobs for a given hour.
//     *
//     * @param args the command line arguments
//     */
//    public static void main(String[] args) {
//        long oneHourInNanos = 3_600_000_000_000L;
//        long hours = Long.MAX_VALUE / oneHourInNanos;
//        System.out.println("hours = " + hours);
//        long days = hours / 24;
//        System.out.println("days = " + days);
//        long years = days / 365;
//        System.out.println("years = " + years);
////        listHour(oneHourInNanos*24*365*5).forEach(chainFile -> {
//        listHour(0).forEach(System.out::println);
//    }
}
