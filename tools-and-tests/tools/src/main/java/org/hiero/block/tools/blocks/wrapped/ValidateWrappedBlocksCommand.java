// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;

import com.hedera.hapi.block.stream.Block;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
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
 *
 * <p>Balance validation uses pre-fetched checkpoint files created by
 * {@link FetchBalanceCheckpointsCommand} or custom balance files from saved states.
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
            description = "Enable validation of account balances (enabled by default)",
            defaultValue = "true",
            negatable = true)
    private boolean validateBalances = true;

    @Option(
            names = {"--balance-checkpoints"},
            description = "Path to pre-fetched balance checkpoints file (balance_checkpoints.zstd)")
    private Path balanceCheckpointsFile;

    @Option(
            names = {"--custom-balances-dir"},
            description = "Directory containing custom balance files (accountBalances_{blockNumber}.pb.gz)")
    private Path customBalancesDir;

    @Option(
            names = {"--balance-check-interval-days"},
            description = "Only validate balance checkpoints every N days (default: 30 = monthly)",
            defaultValue = "30")
    private int balanceCheckIntervalDays;

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

        // Initialize balance validator if enabled
        BalanceCheckpointValidator balanceCheckpointValidator = null;

        if (validateBalances) {
            System.out.println(Ansi.AUTO.string("@|yellow Balance validation:|@ enabled"));
            System.out.println(
                    Ansi.AUTO.string("@|yellow Balance check interval:|@ every " + balanceCheckIntervalDays + " days"));

            try {
                balanceCheckpointValidator = new BalanceCheckpointValidator();
                balanceCheckpointValidator.setCheckIntervalDays(balanceCheckIntervalDays);

                // Load from checkpoint file if specified
                if (balanceCheckpointsFile != null && Files.exists(balanceCheckpointsFile)) {
                    System.out.println(
                            Ansi.AUTO.string("@|yellow Balance source:|@ checkpoint file: " + balanceCheckpointsFile));
                    balanceCheckpointValidator.loadFromFile(balanceCheckpointsFile);
                }

                // Load from custom balances directory if specified
                if (customBalancesDir != null && Files.isDirectory(customBalancesDir)) {
                    System.out.println(Ansi.AUTO.string("@|yellow Custom balances dir:|@ " + customBalancesDir));
                    balanceCheckpointValidator.loadFromDirectory(customBalancesDir);
                }

                // If no checkpoints loaded, disable balance validation
                if (balanceCheckpointValidator.getCheckpointCount() == 0) {
                    System.out.println(Ansi.AUTO.string(
                            "@|yellow Warning:|@ No balance checkpoints loaded, skipping balance validation"));
                    System.out.println(Ansi.AUTO.string(
                            "@|yellow Hint:|@ Run 'blocks fetchBalanceCheckpoints' to download checkpoints, "
                                    + "or specify --balance-checkpoints or --custom-balances-dir"));
                    balanceCheckpointValidator = null;
                }
            } catch (IOException e) {
                System.err.println(Ansi.AUTO.string("@|red Error loading balance checkpoints:|@ " + e.getMessage()));
                return 1;
            }
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

                // Validate against balance checkpoints if enabled
                if (balanceCheckpointValidator != null && balanceMap != null) {
                    balanceCheckpointValidator.checkBlock(blockNumber, balanceMap);
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
        if (balanceCheckpointValidator != null) {
            balanceCheckpointValidator.printSummary();
            if (!balanceCheckpointValidator.allPassed()) {
                return 1;
            }
        }

        System.out.println();
        System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        return 0;
    }
}
