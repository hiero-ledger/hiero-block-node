// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import com.hedera.hapi.block.stream.Block;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.validation.BalanceCheckpointValidation;
import org.hiero.block.tools.blocks.validation.BlockChainValidation;
import org.hiero.block.tools.blocks.validation.BlockStructureValidation;
import org.hiero.block.tools.blocks.validation.BlockValidation;
import org.hiero.block.tools.blocks.validation.HbarSupplyValidation;
import org.hiero.block.tools.blocks.validation.HistoricalBlockTreeValidation;
import org.hiero.block.tools.blocks.validation.RequiredItemsValidation;
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
 * <p>Walks all blocks in the input directory in order and validates each one using a list of
 * {@link BlockValidation} instances. The input directory must be a directory written by
 * {@link BlockWriter} and readable with {@link BlockReader}.
 *
 * <p>When the directory starts at block zero, genesis-requiring validations (historical block
 * tree, HBAR supply, balance checkpoints) are enabled. When starting from a later block,
 * only chain and structural validations run.
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

        // Build validation list
        final List<BlockValidation> validations = new ArrayList<>();
        final BlockChainValidation chainValidation = new BlockChainValidation();
        validations.add(chainValidation);
        validations.add(new RequiredItemsValidation());
        validations.add(new BlockStructureValidation());

        // Balance checkpoint validation reference (for summary printing)
        BalanceCheckpointValidation balanceCheckpointValidation = null;

        if (startsAtZero) {
            validations.add(new HistoricalBlockTreeValidation(chainValidation));

            final HbarSupplyValidation supplyValidation = new HbarSupplyValidation();
            validations.add(supplyValidation);

            // Initialize balance checkpoint validator if enabled
            if (validateBalances) {
                System.out.println(Ansi.AUTO.string("@|yellow Balance validation:|@ enabled"));
                System.out.println(Ansi.AUTO.string(
                        "@|yellow Balance check interval:|@ every " + balanceCheckIntervalDays + " days"));

                try {
                    final BalanceCheckpointValidator checkpointValidator = new BalanceCheckpointValidator();
                    checkpointValidator.setCheckIntervalDays(balanceCheckIntervalDays);

                    if (balanceCheckpointsFile != null && Files.exists(balanceCheckpointsFile)) {
                        System.out.println(Ansi.AUTO.string(
                                "@|yellow Balance source:|@ checkpoint file: " + balanceCheckpointsFile));
                        checkpointValidator.loadFromFile(balanceCheckpointsFile);
                    }

                    if (customBalancesDir != null && Files.isDirectory(customBalancesDir)) {
                        System.out.println(Ansi.AUTO.string("@|yellow Custom balances dir:|@ " + customBalancesDir));
                        checkpointValidator.loadFromDirectory(customBalancesDir);
                    }

                    loadBundledCheckpoints(checkpointValidator);

                    if (checkpointValidator.getCheckpointCount() > 0) {
                        balanceCheckpointValidation =
                                new BalanceCheckpointValidation(supplyValidation.getAccounts(), checkpointValidator);
                        validations.add(balanceCheckpointValidation);
                    } else {
                        System.out.println(Ansi.AUTO.string(
                                "@|yellow Warning:|@ No balance checkpoints loaded, skipping balance validation"));
                        System.out.println(Ansi.AUTO.string(
                                "@|yellow Hint:|@ Run 'blocks fetchBalanceCheckpoints' to download checkpoints, "
                                        + "or specify --balance-checkpoints or --custom-balances-dir"));
                    }
                } catch (IOException e) {
                    System.err.println(
                            Ansi.AUTO.string("@|red Error loading balance checkpoints:|@ " + e.getMessage()));
                    return 1;
                }
            }
        }
        System.out.println();

        // Init all validations
        for (final BlockValidation validation : validations) {
            validation.init(firstBlock);
        }

        // Validation tracking
        final long startNanos = System.nanoTime();
        long blocksValidated = 0;

        // Walk all blocks in order
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            try {
                final Block block = BlockReader.readBlock(inputDir, blockNumber);

                // Phase 1: validate (no state committed)
                for (final BlockValidation validation : validations) {
                    validation.validate(block, blockNumber);
                }

                // Phase 2: commit state (all validations passed)
                for (final BlockValidation validation : validations) {
                    validation.commitState(block, blockNumber);
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

        // Cleanup
        for (final BlockValidation validation : validations) {
            validation.close();
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
        if (balanceCheckpointValidation != null) {
            balanceCheckpointValidation.getCheckpointValidator().printSummary();
            if (!balanceCheckpointValidation.getCheckpointValidator().allPassed()) {
                return 1;
            }
        }

        System.out.println();
        System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        return 0;
    }

    /**
     * Load bundled balance checkpoint files from resources.
     * These are accountBalances_{blockNumber}.pb.gz files compiled into the jar.
     */
    private void loadBundledCheckpoints(BalanceCheckpointValidator validator) throws IOException {
        String[] bundledFiles = {"accountBalances_91019204.pb.gz"};

        for (String filename : bundledFiles) {
            try (InputStream stream = getClass().getResourceAsStream("/metadata/" + filename)) {
                if (stream != null) {
                    String blockStr = filename.replace("accountBalances_", "").replace(".pb.gz", "");
                    long blockNumber = Long.parseLong(blockStr);
                    validator.loadFromGzippedStream(stream, blockNumber);
                    System.out.println(Ansi.AUTO.string("@|yellow Loaded bundled checkpoint:|@ block " + blockNumber));
                }
            }
        }
    }
}
