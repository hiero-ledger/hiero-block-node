// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks;

import static org.hiero.block.tools.commands.record2blocks.mirrornode.FetchBlockQuery.getPreviousHashForBlock;
import static org.hiero.block.tools.blocks.BlockWriter.writeBlock;
import static org.hiero.block.tools.records.RecordFileDates.blockTimeLongToInstant;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hiero.block.tools.commands.record2blocks.gcp.MainNetBucket;
import org.hiero.block.tools.commands.record2blocks.model.BlockInfo;
import org.hiero.block.tools.commands.record2blocks.model.BlockTimes;
import org.hiero.block.tools.commands.record2blocks.model.ChainFile;
import org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile;
import org.hiero.block.tools.records.RecordFileInfo;
import org.hiero.block.tools.blocks.BlockWriter.BlockPath;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Command line command that converts a record stream to blocks
 * <p>
 * Example block ranges for testing:
 * <ul>
 *     <li><code>-s 0 -e 10</code> - Record File v2</li>
 *     <li><code>-s 12877843 -e 12877853</code> - Record File v5</li>
 *     <li><code>-s 72756872 -e 72756882</code> - Record File v6 with sidecars</li>
 * </ul>
 * Record files start at V2 at block 0 then change to V5 at block 12370838 and V6 at block 38210031
 */
@SuppressWarnings({"FieldCanBeLocal", "CallToPrintStackTrace"})
@Command(name = "record2block", description = "Converts a record stream files into blocks")
public class Record2BlockCommand implements Runnable {

    @Option(
            names = {"-s", "--start-block"},
            description = "The block to start converting from")
    private int startBlock = 0;

    @Option(
            names = {"-e", "--end-block"},
            description = "The block to end converting at")
    private int endBlock = 3001;

    @Option(
            names = {"-j", "--json"},
            description = "also output blocks as json")
    private boolean jsonEnabled = false;

    @Option(
            names = {"-c", "--cache-enabled"},
            description = "Use local cache for downloaded content")
    private boolean cacheEnabled = false;

    @Option(
            names = {"--min-node-account-id"},
            description = "the account id of the first node in the network")
    private int minNodeAccountId = 3;

    @Option(
            names = {"--max-node-account-id"},
            description = "the account id of the last node in the network")
    private int maxNodeAccountId = 34;

    @Option(
            names = {"-d", "--data-dir"},
            description = "the data directory for output and temporary files")
    private Path dataDir = Path.of("data");

    /** The path to the block times file. */
    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file.")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    /**
     * Path to the output blocks directory
     */
    private Path blocksDir;

    /**
     * Path to the output json blocks directory
     */
    private Path blocksJsonDir;

    /**
     * Empty Default constructor to remove JavaDoc warning
     */
    public Record2BlockCommand() {}

    /**
     * Main method to run the command
     */
    @Override
    public void run() {
        // create executor service
        try (final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
                final ExecutorService singleThreadWritingExecutor = Executors.newSingleThreadExecutor()) {
            blocksDir = dataDir.resolve("blocks");
            blocksJsonDir = dataDir.resolve("blocks-json");
            // enable cache, disable if doing large batches
            final MainNetBucket mainNetBucket =
                    new MainNetBucket(cacheEnabled, dataDir.resolve("gcp-cache"), minNodeAccountId, maxNodeAccountId);
            // create blocks dir
            Files.createDirectories(blocksDir);
            if (jsonEnabled) {
                Files.createDirectories(blocksJsonDir);
            }
            // check start block is less than end block
            if (startBlock > endBlock) {
                throw new IllegalArgumentException("Start block must be less than end block");
            }
            // check blockTimesFile exists
            if (!Files.exists(blockTimesFile)) {
                throw new IllegalArgumentException("Block times file does not exist: " + blockTimesFile);
            }
            // map the block_times.bin file
            final BlockTimes blockTimes = new BlockTimes(blockTimesFile);
            // get previous block hash
            Bytes previousBlockHash;
            if (startBlock == 0) {
                previousBlockHash = Bytes.wrap(new byte[48]); // empty hash for first block
            } else {
                // get previous block hash from mirror node
                previousBlockHash = getPreviousHashForBlock(startBlock);
            }
            // iterate over the blocks
            Instant currentHour = null;
            List<ChainFile> currentHoursFiles = null;
            for (int blockNumber = startBlock; blockNumber <= endBlock; blockNumber++) {
                final int finalBlockNumber = blockNumber;
                // get the time of the record file for this block, from converted mirror node data
                final long blockTime = blockTimes.getBlockTime(blockNumber);
                final Instant blockTimeInstant = blockTimeLongToInstant(blockTime);
                System.out.printf(
                        Ansi.AUTO.string("@|bold,green,underline Processing block|@ %d"
                                + " @|green at blockTime|@ %s"
                                + " @|cyan Progress = block %d of %d" + " = %.2f%% |@\n"),
                        blockNumber,
                        blockTimeInstant,
                        blockNumber - startBlock + 1,
                        endBlock - startBlock + 1,
                        ((double) (blockNumber - startBlock) / (double) (endBlock - startBlock)) * 100d);
                // round instant to nearest hour
                Instant blockTimeHour = blockTimeInstant.truncatedTo(ChronoUnit.HOURS);
                // check if we are the same hour as last block, if not load the new hour
                if (currentHour == null || !currentHour.equals(blockTimeHour)) {
                    currentHour = blockTimeHour;
                    currentHoursFiles = mainNetBucket.listHour(blockTime);
                    System.out.println(Ansi.AUTO.string(
                            "\r@|bold,yellow    Listed " + currentHoursFiles.size() + " files from GCP|@"));
                }
                // create block info
                BlockInfo blockInfo = new BlockInfo(
                        blockNumber,
                        blockTime,
                        currentHoursFiles.stream()
                                .filter(cf -> cf.blockTime() == blockTime)
                                .toList());
                // print block info
                System.out.println("   " + blockInfo);

                // The next 3 steps we do in background threads as they all download files from GCP which can be slow

                // now we need to download the most common record file & parse version information out of record file
                final Future<RecordFileInfo> recordFileInfoFuture = executorService.submit(() -> RecordFileInfo.parse(
                        blockInfo.mostCommonRecordFile().chainFile().download(mainNetBucket)));

                // download and parse all signature files then  convert signature files to list of RecordFileSignatures
                final List<Future<RecordFileSignature>> recordFileSignatureFutures = blockInfo.signatureFiles().stream()
                        .map(cf -> executorService.submit(() -> {
                            final ParsedSignatureFile sigFile = ParsedSignatureFile.downloadAndParse(cf, mainNetBucket);
                            return new RecordFileSignature(Bytes.wrap(sigFile.signature()), sigFile.nodeId());
                        }))
                        .toList();

                // download most common sidecar files, one for each numbered sidecar
                final List<Future<SidecarFile>> sideCarsFutures = blockInfo.sidecarFiles().values().stream()
                        .map(sidecarFile -> executorService.submit(() -> {
                            byte[] sidecarFileBytes = sidecarFile
                                    .mostCommonSidecarFile()
                                    .chainFile()
                                    .download(mainNetBucket);
                            try {
                                return SidecarFile.PROTOBUF.parse(Bytes.wrap(sidecarFileBytes));
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                        }))
                        .toList();

                // collect all background computed data from futures
                final RecordFileInfo recordFileVersionInfo = recordFileInfoFuture.get();
                final List<RecordFileSignature> recordFileSignatures = getResults(recordFileSignatureFutures);
                final List<SidecarFile> sideCars = getResults(sideCarsFutures);

                // build new block
                final BlockHeader blockHeader = new BlockHeader(
                        recordFileVersionInfo.hapiProtoVersion(),
                        recordFileVersionInfo.hapiProtoVersion(),
                        blockNumber,
                        new Timestamp(blockTimeInstant.getEpochSecond(), blockTimeInstant.getNano()),
                        BlockHashAlgorithm.SHA2_384);
                final RecordFileItem recordFileItem = new RecordFileItem(
                        new Timestamp(blockTimeInstant.getEpochSecond(), blockTimeInstant.getNano()),
                        Bytes.wrap(recordFileVersionInfo.recordFileContents()),
                        sideCars,
                        recordFileSignatures);
                final Block block = new Block(List.of(
                        new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, blockHeader)),
                        new BlockItem(new OneOf<>(ItemOneOfType.RECORD_FILE, recordFileItem))));

                // write block to disk on a single threaded executor. This allows the loop to carry on and start
                // downloading files for the next block. We should be download bound so optimizing to keep the queue of
                // downloads as busy as possible.
                singleThreadWritingExecutor.submit(() -> {
                    try {
                        final BlockPath blockPath = writeBlock(blocksDir, block);
                        // write as json for now as well
                        if (jsonEnabled) {
                            final Path blockJsonPath = blocksJsonDir.resolve(blockPath.blockNumStr() + ".blk.json");
                            Files.createDirectories(blockJsonPath.getParent());
                            try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(
                                    blockJsonPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE))) {
                                Block.JSON.write(block, out);
                            }
                            System.out.println(Ansi.AUTO.string(
                                    "@|bold,yellow    Wrote block [|@" + finalBlockNumber + "@|bold,yellow ]to|@ "
                                            + blockPath.dirPath()
                                            + "/" + blockPath.zipFileName() + "@|bold,cyan :|@"
                                            + blockPath.blockFileName() + "@|bold,yellow ] and json to|@ "
                                            + blockJsonPath));
                        } else {
                            System.out.println(Ansi.AUTO.string(
                                    "@|bold,yellow    Wrote block [|@" + finalBlockNumber + "@|bold,yellow ]to|@ "
                                            + blockPath.dirPath()
                                            + "/" + blockPath.zipFileName() + "@|bold,cyan :|@"
                                            + blockPath.blockFileName()));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                });
                // update previous block hash
                previousBlockHash = recordFileVersionInfo.blockHash();
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get list results of a list of future
     *
     * @param futures list of futures to collect results from
     * @return list of results
     * @param <I> the type of the future
     */
    private static <I> List<I> getResults(List<Future<I>> futures) {
        try {
            List<I> results = new ArrayList<>(futures.size());
            for (Future<I> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
