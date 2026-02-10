// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.Timestamp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * CLI subcommand that validates wrapped block stream files produced by
 * {@link org.hiero.block.tools.blocks.ToWrappedBlocksCommand}.
 *
 * <p>Walks all blocks in the input directory in order and validates each one using
 * {@link WrappedBlockValidator#validateBlock}. The input directory must be a directory written by
 * {@link BlockWriter} and readable with {@link BlockReader}.
 *
 * <p>When the directory starts at block zero a {@link StreamingHasher} is created to validate the
 * historical block hash merkle tree. When starting from a later block, merkle tree validation is
 * skipped because the prior tree state is unavailable.
 */
@SuppressWarnings({"FieldCanBeLocal", "DuplicatedCode"})
@Command(
        name = "validate-wrapped",
        description = "Validate wrapped block stream blocks produced by the wrap command",
        mixinStandardHelpOptions = true)
public class ValidateWrappedBlocksCommand implements Callable<Integer> {

    @SuppressWarnings("unused")
    @Parameters(index = "0..1", description = "Block files, directories, or zip archives to process")
    private File[] files;

    @Option(
            names = {"--validate-balances"},
            description = "Enable validation of account balances against CSV balance files from GCP")
    private boolean validateBalances = false;

    @Option(
            names = {"--balance-start-day"},
            description = "Start day for balance validation in format YYYY-MM-DD (e.g., 2019-09-13)")
    private String balanceStartDay;

    @Option(
            names = {"--balance-end-day"},
            description = "End day for balance validation in format YYYY-MM-DD (e.g., 2024-12-31)")
    private String balanceEndDay;

    @Option(
            names = {"--gcp-project"},
            description = "GCP project for requester-pays bucket access")
    private String gcpProject;

    @Option(
            names = {"--cache-dir"},
            description = "Directory for caching downloaded files",
            defaultValue = "data/gcp-cache")
    private Path cacheDir;

    @Option(
            names = {"--min-node"},
            description = "Minimum node account ID",
            defaultValue = "3")
    private int minNodeAccountId;

    @Option(
            names = {"--max-node"},
            description = "Maximum node account ID",
            defaultValue = "34")
    private int maxNodeAccountId;

    @Option(
            names = {"--address-book"},
            description = "Path to address book history JSON file for signature verification")
    private Path addressBookPath;

    @Option(
            names = {"--verify-signatures"},
            description = "Verify balance file signatures (requires --address-book)",
            defaultValue = "false")
    private boolean verifySignatures;

    @Override
    public Integer call() {
        if (files == null || files.length == 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No input directory specified"));
            return 1;
        }
        final Path inputDir = files[0].toPath();
        // Validate input directory exists
        if (!Files.isDirectory(inputDir)) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ Input directory does not exist: " + inputDir));
            return 1;
        }

        // Discover block range - try zip archives first, then individual files, ZSTD then NONE
        long firstBlock = BlockWriter.minStoredBlockNumber(inputDir, CompressionType.ZSTD);
        long lastBlock = BlockWriter.maxStoredBlockNumber(inputDir, CompressionType.ZSTD);
        if (firstBlock < 0) {
            firstBlock = BlockWriter.minStoredBlockNumber(inputDir, CompressionType.NONE);
            lastBlock = BlockWriter.maxStoredBlockNumber(inputDir, CompressionType.NONE);
        }
        if (firstBlock < 0) {
            firstBlock = BlockFile.nestedDirectoriesMinBlockNumber(inputDir, CompressionType.ZSTD);
            lastBlock = BlockFile.nestedDirectoriesMaxBlockNumber(inputDir, CompressionType.ZSTD);
        }
        if (firstBlock < 0) {
            firstBlock = BlockFile.nestedDirectoriesMinBlockNumber(inputDir, CompressionType.NONE);
            lastBlock = BlockFile.nestedDirectoriesMaxBlockNumber(inputDir, CompressionType.NONE);
        }
        if (firstBlock < 0 || lastBlock < 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No blocks found in: " + inputDir.toAbsolutePath()));
            return 1;
        }

        final long totalBlocks = lastBlock - firstBlock + 1;
        final boolean startsAtZero = firstBlock == 0;

        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   WRAPPED BLOCK VALIDATION|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow Input directory:|@ " + inputDir.toAbsolutePath()));
        System.out.println(Ansi.AUTO.string("@|yellow Block range:|@ " + firstBlock + " - " + lastBlock));
        System.out.println(Ansi.AUTO.string("@|yellow Total blocks:|@ " + totalBlocks));
        if (startsAtZero) {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Historical Block hash tree validation:|@ enabled (starts at block 0)"));
        } else {
            System.out.println(Ansi.AUTO.string(
                    "@|yellow Historical Block hash tree validation:|@ disabled (starts at block " + firstBlock + ")"));
        }

        // Initialize balance CSV validator if enabled
        BalanceCsvValidator balanceCsvValidator = null;
        if (validateBalances) {
            if (balanceStartDay == null || balanceEndDay == null) {
                System.err.println(
                        Ansi.AUTO.string(
                                "@|red Error:|@ --balance-start-day and --balance-end-day are required when --validate-balances is enabled"));
                return 1;
            }
            if (verifySignatures && addressBookPath == null) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ --address-book is required when --verify-signatures is enabled"));
                return 1;
            }
            System.out.println(Ansi.AUTO.string("@|yellow Balance validation:|@ enabled"));
            System.out.println(
                    Ansi.AUTO.string("@|yellow Balance date range:|@ " + balanceStartDay + " to " + balanceEndDay));
            if (verifySignatures) {
                System.out.println(Ansi.AUTO.string("@|yellow Signature verification:|@ enabled"));
            }

            // Load address book registry if signature verification is enabled
            AddressBookRegistry addressBookRegistry = null;
            if (verifySignatures && addressBookPath != null) {
                System.out.println(Ansi.AUTO.string("@|yellow Loading address book:|@ " + addressBookPath));
                addressBookRegistry = new AddressBookRegistry(addressBookPath);
            }

            balanceCsvValidator = new BalanceCsvValidator(
                    cacheDir, minNodeAccountId, maxNodeAccountId, gcpProject, addressBookRegistry, verifySignatures);
            balanceCsvValidator.loadCheckpoints(balanceStartDay, balanceEndDay);
        }
        System.out.println();

        // Create a streaming hasher and balance map only if we start from block 0
        final StreamingHasher streamingHasher = startsAtZero ? new StreamingHasher() : null;
        final Map<Long, Long> balanceMap = startsAtZero ? new HashMap<>() : null;

        // Validation tracking
        final long startNanos = System.nanoTime();
        long blocksValidated = 0;
        byte[] previousBlockHash = null;

        // Walk all blocks in order
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            try {
                final Block block = BlockReader.readBlock(inputDir, blockNumber);
                WrappedBlockValidator.validateBlock(block, blockNumber, previousBlockHash, streamingHasher, balanceMap);

                // Validate against balance CSV checkpoints if enabled
                if (balanceCsvValidator != null && balanceMap != null) {
                    Instant blockTimestamp = extractBlockTimestamp(block);
                    if (blockTimestamp != null) {
                        balanceCsvValidator.checkBlock(blockTimestamp, balanceMap);
                    }
                }

                // Compute block hash and update state for the next block's validation
                previousBlockHash = hashBlock(block);
                if (streamingHasher != null) {
                    streamingHasher.addNodeByHash(previousBlockHash);
                }

                blocksValidated++;

                // Print progress every 1000 blocks
                if (blocksValidated % 1000 == 0 || blockNumber == lastBlock) {
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    long remainingMillis =
                            PrettyPrint.computeRemainingMilliseconds(blocksValidated, totalBlocks, elapsedMillis);
                    double percent = ((double) blocksValidated / (double) totalBlocks) * 100.0;
                    String progressString = String.format("Validated %d/%d blocks", blocksValidated, totalBlocks);
                    PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                }
            } catch (IOException e) {
                PrettyPrint.clearProgress();
                System.err.println(
                        Ansi.AUTO.string("@|red Error reading block " + blockNumber + ":|@ " + e.getMessage()));
                return 1;
            } catch (ValidationException e) {
                PrettyPrint.clearProgress();
                System.err.println(Ansi.AUTO.string("@|red Block " + blockNumber + ":|@ " + e.getMessage()));
                return 1;
            }
        }

        // Print summary
        PrettyPrint.clearProgress();
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   VALIDATION SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow Blocks validated:|@ " + blocksValidated));

        if (blocksValidated == 0) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,yellow No blocks found in:|@ " + inputDir.toAbsolutePath()));
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        }

        long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000L;
        System.out.println(Ansi.AUTO.string("@|yellow Time elapsed:|@ " + elapsedSeconds + " seconds"));

        // Print balance validation summary if enabled
        if (balanceCsvValidator != null) {
            balanceCsvValidator.printSummary();
            if (!balanceCsvValidator.allPassed()) {
                return 1;
            }
        }

        System.out.println();
        System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        return 0;
    }

    /**
     * Extract the consensus timestamp from a block's header.
     *
     * @param block the block to extract timestamp from
     * @return the block timestamp as Instant, or null if not found
     */
    private static Instant extractBlockTimestamp(Block block) {
        return block.items().stream()
                .filter(BlockItem::hasBlockHeader)
                .findFirst()
                .map(item -> {
                    Timestamp ts = item.blockHeaderOrThrow().blockTimestamp();
                    if (ts == null) {
                        return null;
                    }
                    return Instant.ofEpochSecond(ts.seconds(), ts.nanos());
                })
                .orElse(null);
    }
}
