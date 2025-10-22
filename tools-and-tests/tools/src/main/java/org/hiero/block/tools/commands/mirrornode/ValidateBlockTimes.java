// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

import static org.hiero.block.tools.records.RecordFileDates.blockTimeLongToRecordFilePrefix;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.commands.record2blocks.model.BlockTimes;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Validate the block times in the block_times.bin file by comparing them to the record file names in CSV.
 */
@Command(name = "validateBlockTimes", description = "Validates a block times file")
public class ValidateBlockTimes implements Runnable {

    /** The path to the record table CSV from mirror node, gzipped. */
    @Option(
            names = {"--record-csv"},
            description = "Path to the record table CSV from mirror node, gzipped.")
    private Path recordsCsvFile = Path.of("data/record_file.csv.gz");

    /** The path to the block times file. */
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file.")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    /**
     * Read the record file table CSV file and validate the block times in the block times file.
     */
    @Override
    public void run() {
        try {
            final BlockTimes blockTimes = new BlockTimes(blockTimesFile);
            // count the number of blocks to print progress
            final AtomicInteger blockCount = new AtomicInteger(0);
            // read the record file table CSV file
            try (var reader = new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(new FileInputStream(recordsCsvFile.toFile()))))) {
                // skip header
                reader.readLine();
                // read all lines
                reader.lines().parallel().forEach(line -> {
                    final String[] parts = line.split(",");
                    final String recordStreamFileName = parts[0];
                    final int blockNumber = Integer.parseInt(parts[15]);
                    // get the block offset from blockTimes
                    final long blockTime = blockTimes.getBlockTime(blockNumber);
                    // convert the block time to a string without 'Z' on the end
                    String blockTimeString = blockTimeLongToRecordFilePrefix(blockTime);
                    // check the file name starts with the block time
                    if (!recordStreamFileName.startsWith(blockTimeString)) {
                        System.err.printf(
                                "Block %d has incorrect time %s should be %s%n",
                                blockNumber, recordStreamFileName, blockTimeString);
                    }
                    // print progress
                    int currentBlockCount = blockCount.incrementAndGet();
                    if (currentBlockCount % 100_000 == 0) {
                        System.out.printf(
                                "\rblock %,10d - %2.1f%% complete",
                                currentBlockCount, (currentBlockCount / 70_000_000f) * 100);
                    }
                });
            }
        } catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            System.exit(1);
        }
    }
}
