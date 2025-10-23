// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

import static org.hiero.block.tools.records.RecordFileDates.recordFileNameToBlockTimeLong;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Read the record_file.csv.gz file from mirror node and extract the block times into a file.
 * <p>
 * The block times file is a binary file of longs, each long is the number of nanoseconds for that block after first
 * block time. So first block = 0, second about 5 seconds later etc. The index is the block number, so block 0 is first
 * long, block 1 is second block and so on.
 * </p>
 */
@SuppressWarnings({"DuplicatedCode", "CallToPrintStackTrace"})
@Command(name = "extractBlockTimes", description = "Extract block times from mirror node records csv file")
public class ExtractBlockTimes implements Runnable {
    /** the number of blocks in the record CSV file roughly, used for progress estimation */
    private static final int NUMBER_OF_BLOCKS_ROUNDED_UP = 90_000_000;

    /** The path download the record table CSVs from mirror node to, gzipped. */
    @Option(
        names = {"--record-dir"},
        description = "Path to download the record table CSVs from mirror node to, gzipped.")
    private Path recordsCsvDir = Path.of("data/mirror_node_record_files");

    /** The path to the block times file. */
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file.")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    /**
     * Read the record file table CSV file and extract the block times into a file.
     */
    @Override
    public void run() {
        // get the start time of the first block
        // create off heap array to store the block times
        final ByteBuffer blockTimesBytes = ByteBuffer.allocateDirect(NUMBER_OF_BLOCKS_ROUNDED_UP * Long.BYTES);
        final LongBuffer blockTimes = blockTimesBytes.asLongBuffer();
        // count the number of blocks to print progress
        final AtomicInteger blockCount = new AtomicInteger(0);
        // find the record files subdirectory
        final Path recordsCsvRootPath = recordsCsvDir.toAbsolutePath();
        final String latestVersion;
        try (var listStream = Files.list(recordsCsvRootPath)) {
            latestVersion = listStream
                .map(path -> path.getFileName().toString())
                .filter(pathStr -> MirrorNodeUtils.SYMANTIC_VERSION_PATTERN.matcher(pathStr).matches())
                .max(Comparator.comparingLong(MirrorNodeUtils::parseSymantecVersion)).orElseThrow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final Path recordsCsvDirPath = recordsCsvRootPath.resolve(latestVersion).resolve("record_file");
        // read all record files in the directory
        try(var listStream = Files.list(recordsCsvDirPath)) {
            final List<Path> recordsCsvFiles = listStream
                .filter(path -> path.getFileName().toString().startsWith("record_file"))
                .filter(path -> path.getFileName().toString().endsWith(".csv.gz"))
                .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                .toList();
            System.out.println("Found " + recordsCsvFiles.size() + " record CSV files in " + recordsCsvDirPath);
            for(Path recordsCsvFile : recordsCsvFiles) {
                // read the record file table CSV file

                // read the record file table CSV file
                try (var reader = new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(new FileInputStream(recordsCsvFile.toFile()))))) {
                    // skip header
                    final String headerLine = reader.readLine();
                    final String[] headerLineParts = headerLine.split(",");
                    // example header:
                    // bytes,consensus_start,consensus_end,count,digest_algorithm,file_hash,gas_used,hapi_version_major,
                    // hapi_version_minor,hapi_version_patch,hash,index,load_start,load_end,logs_bloom,name,node_id,
                    // prev_hash,sidecar_count,size,version,software_version_major,software_version_minor,
                    // software_version_patch,round_start,round_end
                    // find index of record_stream_file_name and block_number
                    int recordStreamFileNameIndex = -1;
                    int blockNumberIndex = -1;
                    for (int i = 0; i < headerLineParts.length; i++) {
                        if (headerLineParts[i].equals("name")) {
                            recordStreamFileNameIndex = i;
                        } else if (headerLineParts[i].equals("index")) {
                            blockNumberIndex = i;
                        }
                    }
                    final int finalRecordStreamFileNameIndex = recordStreamFileNameIndex;
                    final int finalBlockNumberIndex = blockNumberIndex;
                    // read all lines
                    reader.lines().parallel().forEach(line -> {
                        final String[] parts = line.split(",");
                        final String recordStreamFileName = parts[finalRecordStreamFileNameIndex];
                        final int blockNumber = Integer.parseInt(parts[finalBlockNumberIndex]);
                        // compute nanoseconds since the first block
                        final long nanoseconds = recordFileNameToBlockTimeLong(recordStreamFileName);
                        // write the block time to the off heap array
                        blockTimes.put(blockNumber, nanoseconds);
                        // print progress
                        int currentBlockCount = blockCount.incrementAndGet();
                        if (currentBlockCount % 100_000 == 0) {
                            System.out.printf(
                                "\rblock %,10d - %2.1f%% complete - Processing file: %s",
                                currentBlockCount, (currentBlockCount / 70_000_000f) * 100,
                                recordsCsvFile.getFileName().toString());
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("\nTotal blocks read = " + blockCount.get());
        // set limit to the number of blocks read
        final int totalBlockTimesBytes = blockCount.get() * Long.BYTES;
        blockTimesBytes.limit(totalBlockTimesBytes);
        blockTimes.limit(blockCount.get());
        // scan the block times to find any blocks missing times
        long totalBlocksWithoutTimes = 0;
        blockTimes.position(0);
        for (int i = 0; i < blockTimes.limit(); i++) {
            if (blockTimes.get(i) == 0) {
                totalBlocksWithoutTimes++;
                System.out.println("block[" + i + "] is missing time - blockTimes[" + blockTimes.get(i) + "] = ");
            }
        }
        System.out.println("\ntotalBlocksWithoutTimes = " + totalBlocksWithoutTimes);
        // write the block times to a file
        blockTimesBytes.position(0);

        try(var out = Files.newByteChannel(blockTimesFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long bytesWritten = out.write(blockTimesBytes);
            System.out.println("bytesWritten = " + bytesWritten);
            if (bytesWritten != totalBlockTimesBytes) {
                System.out.println("ERROR: bytesWritten != totalBlockTimesBytes[" + totalBlockTimesBytes + "]");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
