// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.BlockSource;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.PreValidatedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.validation.AddressBookUpdateValidation;
import org.hiero.block.tools.blocks.validation.BalanceCheckpointValidation;
import org.hiero.block.tools.blocks.validation.BlockChainValidation;
import org.hiero.block.tools.blocks.validation.BlockStructureValidation;
import org.hiero.block.tools.blocks.validation.BlockValidation;
import org.hiero.block.tools.blocks.validation.HashRegistryValidation;
import org.hiero.block.tools.blocks.validation.HbarSupplyValidation;
import org.hiero.block.tools.blocks.validation.HistoricalBlockTreeValidation;
import org.hiero.block.tools.blocks.validation.JumpstartValidation;
import org.hiero.block.tools.blocks.validation.NodeStakeUpdateValidation;
import org.hiero.block.tools.blocks.validation.ParallelBlockPreprocessor;
import org.hiero.block.tools.blocks.validation.ParallelBlockPreprocessor.PreprocessedData;
import org.hiero.block.tools.blocks.validation.RequiredItemsValidation;
import org.hiero.block.tools.blocks.validation.SignatureBlockStats;
import org.hiero.block.tools.blocks.validation.SignatureStatsCollector;
import org.hiero.block.tools.blocks.validation.SignatureValidation;
import org.hiero.block.tools.blocks.validation.StreamingMerkleTreeValidation;
import org.hiero.block.tools.blocks.wrapped.BalanceCheckpointValidator;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.NodeStakeRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Validation pipeline orchestrator for wrapped block streams.
 *
 * <p>This command discovers block sources (individual files, hierarchical directories, or zip
 * archives), manages a performance-optimized pipeline (multi-threaded decompression, prefetch
 * queue), handles checkpoint/resume for crash recovery, and delegates all validation logic to
 * {@link BlockValidation} implementations.
 *
 * <p>The validation lifecycle has three phases:
 * <ol>
 *   <li><b>Per-block</b> — each block is validated and committed through all {@link BlockValidation}
 *       instances using the two-phase validate/commit protocol.
 *   <li><b>Finalize</b> — after all blocks are processed, each validation's
 *       {@link BlockValidation#finalize(long, long)} method is called for end-of-stream checks
 *       (e.g. comparing accumulated state against saved state files).
 *   <li><b>Cleanup</b> — all validations are closed to release resources.
 * </ol>
 *
 * <p>Validation is fail-fast: the command stops on the first validation error, saves a checkpoint,
 * prints a detailed error report, and exits. Pass {@code --no-resume} to ignore any existing
 * checkpoint and restart from block 0.</p>
 */
@SuppressWarnings({"CallToPrintStackTrace", "FieldCanBeLocal", "DuplicatedCode"})
@Command(
        name = "validate",
        description = "Validates a wrapped block stream (hash chain, signatures, and state files)",
        mixinStandardHelpOptions = true)
public class ValidateBlocksCommand implements Runnable {

    /** Read buffer size for ZipInputStream — tunes sequential HDD I/O. */
    private static final int ZIP_READ_BUFFER = 1 << 20; // 1 MiB

    /** Sentinel for blocks that were skipped (e.g. corrupt zip mid-stream). */
    private static final PreValidatedBlock SKIPPED_BLOCK =
            new PreValidatedBlock(null, -1, null, null, null, null, null);

    /** Number of zip files skipped because their central directory was corrupt. */
    private final AtomicLong corruptZipCount = new AtomicLong(0);

    @SuppressWarnings("unused")
    @Parameters(index = "0..*", description = "Block files or directories to validate")
    private Path[] files;

    @Option(
            names = {"-a", "--address-book"},
            description = "Path to address book history JSON file")
    private Path addressBookFile;

    @Option(
            names = {"-v", "--verbose"},
            description = "Print details for each block")
    private boolean verbose = false;

    @Option(
            names = {"--no-resume"},
            description = "Ignore any existing checkpoint and start validation from scratch")
    private boolean noResume = false;

    @Option(
            names = {"--threads"},
            description = "Decompression + parse threads (default: available CPU cores - 1)")
    private int threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    @Option(
            names = {"--prefetch"},
            description = "Number of blocks to buffer ahead for decompression (default: ${DEFAULT-VALUE})")
    private int prefetch = 256;

    @Option(
            names = {"--skip-signatures"},
            description = "Skip signature validation (only check hash chain and state)")
    private boolean skipSignatures = false;

    @Option(
            names = {"--skip-supply"},
            description = "Skip HBAR supply validation (useful for networks with known transfer list imbalances)")
    private boolean skipSupply = false;

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
    private int balanceCheckIntervalDays = 30;

    /** Lightweight state loaded from a checkpoint JSON file. */
    private record CheckpointState(long lastValidatedBlockNumber, long blocksValidated, byte[] previousBlockHash) {}

    /**
     * Saves checkpoint state for all validations.
     *
     * @param checkpointDir directory to write checkpoint files into
     * @param lastValidatedBlockNumber the last block number that was fully validated
     * @param blocksValidated number of blocks validated so far
     * @param chainValidation the chain validation (provides previousBlockHash)
     * @param validations all validations to save state for
     */
    private static void saveCheckpoint(
            Path checkpointDir,
            long lastValidatedBlockNumber,
            long blocksValidated,
            BlockChainValidation chainValidation,
            List<BlockValidation> validations) {
        // Save each validation's state
        for (BlockValidation v : validations) {
            try {
                v.save(checkpointDir);
            } catch (Exception e) {
                System.err.println("Warning: could not save " + v.name() + " state: " + e.getMessage());
            }
        }
        // Build and save JSON checkpoint atomically
        byte[] previousBlockHash = chainValidation.getPreviousBlockHash();
        JsonObject root = new JsonObject();
        root.addProperty("schemaVersion", 3);
        root.addProperty("lastValidatedBlockNumber", lastValidatedBlockNumber);
        root.addProperty("blocksValidated", blocksValidated);
        root.addProperty(
                "previousBlockHashHex",
                previousBlockHash != null ? Bytes.wrap(previousBlockHash).toHex() : "");
        final String json = new GsonBuilder().setPrettyPrinting().create().toJson(root);
        try {
            HasherStateFiles.saveAtomically(checkpointDir.resolve("validateProgress.json"), path -> {
                try (var writer = Files.newBufferedWriter(path)) {
                    writer.write(json);
                }
            });
        } catch (Exception e) {
            System.err.println("Warning: could not save checkpoint: " + e.getMessage());
        }
    }

    /**
     * Loads checkpoint state from {@code validateProgress.json} inside the given directory,
     * with fallback to {@code validateProgress.json.bak} if the primary file is missing or corrupt.
     *
     * @param checkpointDir directory containing checkpoint files
     * @return the loaded state, or {@code null} if both primary and backup are absent/corrupt
     */
    private static CheckpointState loadCheckpoint(Path checkpointDir) {
        Path jsonFile = checkpointDir.resolve("validateProgress.json");
        CheckpointState[] result = {null};
        HasherStateFiles.loadWithFallback(jsonFile, path -> {
            try (var reader = Files.newBufferedReader(path)) {
                JsonObject root = JsonParser.parseReader(reader).getAsJsonObject();
                byte[] prevHash =
                        HexFormat.of().parseHex(root.get("previousBlockHashHex").getAsString());
                result[0] = new CheckpointState(
                        root.get("lastValidatedBlockNumber").getAsLong(),
                        root.get("blocksValidated").getAsLong(),
                        prevHash);
            }
        });
        return result[0];
    }

    @Override
    public void run() {
        if (files == null || files.length == 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No files to validate"));
            return;
        }
        if (threads <= 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ --threads must be >= 1"));
            return;
        }
        if (prefetch <= 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ --prefetch must be >= 1"));
            return;
        }

        // Auto-detect addressBookHistory.json if not explicitly provided
        if (addressBookFile == null) {
            for (Path file : files) {
                if (Files.isDirectory(file)) {
                    Path potentialAddressBook = file.resolve("addressBookHistory.json");
                    if (Files.exists(potentialAddressBook)) {
                        addressBookFile = potentialAddressBook;
                        System.out.println(
                                Ansi.AUTO.string("@|yellow Auto-detected address book:|@ " + potentialAddressBook));
                        break;
                    }
                }
            }
        }

        // Load the address book registry — required for signature validation
        AddressBookRegistry addressBookRegistry;
        if (addressBookFile != null && Files.exists(addressBookFile)) {
            addressBookRegistry = new AddressBookRegistry(addressBookFile);
            System.out.println(Ansi.AUTO.string("@|yellow Loaded address book from:|@ " + addressBookFile));
        } else if (skipSignatures) {
            addressBookRegistry = null;
        } else {
            // Fall back to the built-in genesis address book for the configured network.
            // AddressBookUpdateValidation will discover subsequent address book changes
            // from the block data as it processes blocks.
            try {
                addressBookRegistry = new AddressBookRegistry();
                System.out.println(Ansi.AUTO.string("@|yellow Using genesis address book for network:|@ "
                        + NetworkConfig.current().networkName()
                        + " (address book updates will be discovered from block data)"));
            } catch (Exception e) {
                System.err.println(
                        Ansi.AUTO.string("@|red Error:|@ No address book found. Provide --address-book, place "
                                + "addressBookHistory.json in the input directory, or set --network."));
                return;
            }
        }

        // Find all block sources (zips are represented as whole-zip sources — one per zip)
        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(files, corruptZipCount);
        if (sources.isEmpty()) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No block files found"));
            return;
        }
        final long estimatedTotalBlocks = BlockZipsUtilities.estimateTotalBlocks(sources);

        // Checkpoint directory lives inside the first input directory
        Path checkpointDir = Arrays.stream(files)
                .filter(Files::isDirectory)
                .map(f -> f.resolve("validateCheckpoint"))
                .findFirst()
                .orElse(Path.of("validateCheckpoint"));

        // Load checkpoint if one exists and --no-resume was not specified
        CheckpointState checkpoint = null;
        if (!noResume && Files.isDirectory(checkpointDir)) {
            checkpoint = loadCheckpoint(checkpointDir);
            if (checkpoint != null) {
                System.out.println(Ansi.AUTO.string("@|yellow Resuming from checkpoint:|@ last validated block = "
                        + checkpoint.lastValidatedBlockNumber()));
            }
        }

        long resumeFrom = checkpoint != null ? checkpoint.lastValidatedBlockNumber() : -1L;
        // Filter sources: for whole-zip sources, keep if the zip might contain blocks > resumeFrom
        final long initialResumeFrom = resumeFrom;
        List<BlockSource> pendingSources = (checkpoint == null)
                ? sources
                : sources.stream()
                        .filter(s -> {
                            if (s.isWholeZip()) {
                                return s.blockNumber() + BlockZipsUtilities.DEFAULT_BLOCKS_PER_ZIP > initialResumeFrom;
                            }
                            return s.blockNumber() > initialResumeFrom;
                        })
                        .toList();

        // Detect binary state files in any input directory
        Path hashRegistryPath = null;
        Path streamingMerkleTreePath = null;
        Path jumpstartPath = null;
        for (Path file : files) {
            if (Files.isDirectory(file)) {
                if (Files.exists(file.resolve("blockStreamBlockHashes.bin"))) {
                    hashRegistryPath = file.resolve("blockStreamBlockHashes.bin");
                    streamingMerkleTreePath = file.resolve("streamingMerkleTree.bin");
                    jumpstartPath = file.resolve("jumpstart.bin");
                    break;
                }
            }
        }
        final boolean hasStateFiles = (hashRegistryPath != null);

        // Compute display range: first block is from first source; last block is estimated from last source
        final long firstDisplayBlock = sources.getFirst().blockNumber();
        final BlockSource lastSource = sources.getLast();
        final long lastDisplayBlock = lastSource.isWholeZip()
                ? lastSource.blockNumber() + BlockZipsUtilities.DEFAULT_BLOCKS_PER_ZIP - 1
                : lastSource.blockNumber();

        System.out.print(Ansi.AUTO.string("""
              @|bold,cyan ════════════════════════════════════════════════════════════|@
              @|bold,cyan   BLOCK STREAM VALIDATION|@
              @|bold,cyan ════════════════════════════════════════════════════════════|@

              @|yellow Estimated total blocks:|@ %d  (sources: %d)
              @|yellow Block range:|@ %d - %d
              @|yellow Threads:|@ %d  prefetch: %d
              """.formatted(
                        estimatedTotalBlocks, sources.size(), firstDisplayBlock, lastDisplayBlock, threads, prefetch)));
        if (hasStateFiles) {
            System.out.println(Ansi.AUTO.string("@|yellow State files found:|@ blockStreamBlockHashes.bin, "
                    + "streamingMerkleTree.bin, jumpstart.bin"));
        }
        if (checkpoint != null) {
            System.out.println(Ansi.AUTO.string("@|yellow Pending sources:|@ " + pendingSources.size()
                    + " (resuming after block " + checkpoint.lastValidatedBlockNumber() + ")"));
        }
        System.out.println();

        // Gap detection for non-zip sources (zips are checked during streaming)
        long expectedBlockNumber = sources.getFirst().blockNumber();
        for (BlockSource source : sources) {
            if (source.isWholeZip()) {
                // For whole-zip sources, check that ranges are contiguous
                if (source.blockNumber() != expectedBlockNumber) {
                    System.out.println(Ansi.AUTO.string("@|red Gap detected:|@ Expected block " + expectedBlockNumber
                            + " but found zip starting at " + source.blockNumber()));
                }
                expectedBlockNumber = source.blockNumber() + BlockZipsUtilities.DEFAULT_BLOCKS_PER_ZIP;
            } else {
                if (source.blockNumber() != expectedBlockNumber) {
                    System.out.println(Ansi.AUTO.string("@|red Gap detected:|@ Expected block " + expectedBlockNumber
                            + " but found " + source.blockNumber()));
                }
                expectedBlockNumber = source.blockNumber() + 1;
            }
        }

        // ── Build validation list ────────────────────────────────────────────
        BlockStreamBlockHashRegistry registry =
                hasStateFiles ? new BlockStreamBlockHashRegistry(hashRegistryPath) : null;

        BlockChainValidation chainValidation = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chainValidation);
        HbarSupplyValidation supplyValidation = skipSupply ? null : new HbarSupplyValidation();
        // Parallel validations (stateless, run in decompPool threads)
        final AddressBookRegistry abRegistry = addressBookRegistry;
        final NodeStakeRegistry nodeStakeRegistry = new NodeStakeRegistry();
        List<BlockValidation> parallelValidations = new ArrayList<>();
        parallelValidations.add(new RequiredItemsValidation());
        parallelValidations.add(new BlockStructureValidation());
        SignatureValidation sigVal = null;
        if (!skipSignatures) {
            sigVal = new SignatureValidation(abRegistry, nodeStakeRegistry, true);
            parallelValidations.add(sigVal);
        } else {
            System.out.println(Ansi.AUTO.string("@|yellow Skipping:|@ Signature validation (--skip-signatures)"));
        }
        final SignatureValidation signatureValidation = sigVal;
        if (skipSupply) {
            System.out.println(Ansi.AUTO.string("@|yellow Skipping:|@ HBAR supply validation (--skip-supply)"));
        }

        // Sequential validations (stateful, run on main thread)
        List<BlockValidation> sequentialValidations = new ArrayList<>();
        // Address book update must come first so the registry is current before other validations
        if (abRegistry != null) {
            sequentialValidations.add(new AddressBookUpdateValidation(abRegistry));
        }
        // Node stake update comes after address book so stake weights are current for signature validation
        sequentialValidations.add(new NodeStakeUpdateValidation(nodeStakeRegistry));
        sequentialValidations.add(chainValidation);
        sequentialValidations.add(treeValidation);
        if (supplyValidation != null) {
            sequentialValidations.add(supplyValidation);
        }
        if (registry != null) {
            sequentialValidations.add(new HashRegistryValidation(registry, chainValidation));
        }
        if (hasStateFiles) {
            sequentialValidations.add(new StreamingMerkleTreeValidation(streamingMerkleTreePath, treeValidation));
            sequentialValidations.add(new JumpstartValidation(jumpstartPath, treeValidation, registry));
        }

        // Balance checkpoint validation (genesis-requiring, optional; requires supply validation)
        if (firstDisplayBlock == 0 && validateBalances && supplyValidation != null) {
            try {
                final BalanceCheckpointValidator checkpointValidator = new BalanceCheckpointValidator();
                checkpointValidator.setCheckIntervalDays(balanceCheckIntervalDays);
                if (balanceCheckpointsFile != null && Files.exists(balanceCheckpointsFile)) {
                    checkpointValidator.loadFromFile(balanceCheckpointsFile);
                }
                if (customBalancesDir != null && Files.isDirectory(customBalancesDir)) {
                    checkpointValidator.loadFromDirectory(customBalancesDir);
                }
                loadBundledCheckpoints(checkpointValidator);
                if (checkpointValidator.getCheckpointCount() > 0) {
                    sequentialValidations.add(
                            new BalanceCheckpointValidation(supplyValidation.getAccounts(), checkpointValidator));
                } else {
                    System.out.println(Ansi.AUTO.string(
                            "@|yellow Warning:|@ No balance checkpoints loaded, skipping balance validation"));
                }
            } catch (IOException e) {
                System.err.println(Ansi.AUTO.string("@|red Error loading balance checkpoints:|@ " + e.getMessage()));
                return;
            }
        }

        // Combined list for checkpoint save/load/finalize/close
        List<BlockValidation> validations = new ArrayList<>();
        validations.addAll(parallelValidations);
        validations.addAll(sequentialValidations);

        // Filter out genesis-required validations when not starting from block 0 and no checkpoint
        if (firstDisplayBlock != 0 && checkpoint == null) {
            List<BlockValidation> genesisSkipped = new ArrayList<>();
            sequentialValidations.removeIf(v -> {
                if (v.requiresGenesisStart()) {
                    genesisSkipped.add(v);
                    return true;
                }
                return false;
            });
            validations.removeIf(genesisSkipped::contains);
            for (BlockValidation skipped : genesisSkipped) {
                System.out.println(Ansi.AUTO.string("@|yellow Skipping:|@ " + skipped.name()
                        + " (requires genesis start, first block is " + firstDisplayBlock + ")"));
            }
        }

        // Restore validation state from checkpoint
        if (checkpoint != null) {
            try {
                Files.createDirectories(checkpointDir);
            } catch (IOException ignored) {
            }
            List<BlockValidation> failedLoads = new ArrayList<>();
            for (BlockValidation v : validations) {
                try {
                    v.load(checkpointDir);
                } catch (Exception e) {
                    System.err.println(Ansi.AUTO.string(
                            "@|yellow Warning:|@ could not load " + v.name() + " state: " + e.getMessage()));
                    if (v.requiresGenesisStart()) {
                        failedLoads.add(v);
                    }
                }
            }
            // The progress JSON is authoritative for the previous block hash — override
            // whatever chainValidation.bin may have loaded (it could be stale or from a
            // different run).
            chainValidation.setPreviousBlockHash(checkpoint.previousBlockHash());
            // If a genesis-required validation's state is lost, we cannot continue —
            // silently skipping it would undermine the completeness guarantee of the validation.
            if (!failedLoads.isEmpty()) {
                for (BlockValidation failed : failedLoads) {
                    System.err.println(Ansi.AUTO.string("@|red Error:|@ " + failed.name()
                            + " checkpoint state is corrupt or missing and cannot be rebuilt without "
                            + "starting from block 0. Delete the checkpoint directory and re-run "
                            + "validation from the beginning, or use --no-resume to ignore the checkpoint."));
                }
                return;
            }
            if (treeValidation.getStreamingHasher().leafCount() > 0) {
                System.out.println(Ansi.AUTO.string("@|yellow Restored streaming hasher:|@ leafCount = "
                        + treeValidation.getStreamingHasher().leafCount()));
            }
        }

        // Cross-validate checkpoint vs registry and adjust if registry is behind
        if (checkpoint != null && registry != null && checkpoint.lastValidatedBlockNumber() >= 0) {
            long checkpointBlock = checkpoint.lastValidatedBlockNumber();
            long registryBlock = registry.highestBlockNumberStored();
            if (registryBlock < checkpointBlock) {
                System.out.println(Ansi.AUTO.string("@|yellow Warning:|@ Registry has blocks up to "
                        + registryBlock + " but checkpoint says " + checkpointBlock
                        + ". Truncating resume point to registry."));
                // Adjust checkpoint so validation re-processes the gap
                checkpoint = new CheckpointState(
                        registryBlock, registryBlock - firstDisplayBlock + 1, registry.mostRecentBlockHash());
                chainValidation.setPreviousBlockHash(checkpoint.previousBlockHash());
                // Recompute resume point and pending sources for the adjusted checkpoint
                resumeFrom = registryBlock;
                final long adjustedResumeFrom = resumeFrom;
                pendingSources = sources.stream()
                        .filter(s -> {
                            if (s.isWholeZip()) {
                                return s.blockNumber() + BlockZipsUtilities.DEFAULT_BLOCKS_PER_ZIP > adjustedResumeFrom;
                            }
                            return s.blockNumber() > adjustedResumeFrom;
                        })
                        .toList();
            }
            // Clean up old completeMerkleTreeValidation checkpoint files (no longer needed)
            for (String suffix : List.of("", ".bak", ".tmp")) {
                Path oldFile = checkpointDir.resolve("completeMerkleTreeValidation.bin" + suffix);
                try {
                    Files.deleteIfExists(oldFile);
                } catch (IOException ignored) {
                }
            }
        }

        // Determine signature stats output directory. If files[0] is a regular file (e.g. a
        // .blk.zip), fall back to its parent directory so the CSV is written alongside it.
        final Path statsOutputDir = Arrays.stream(files)
                .filter(Files::isDirectory)
                .findFirst()
                .orElseGet(() -> files[0].getParent() != null ? files[0].getParent() : Path.of("."));
        final Path statsCsvPath = statsOutputDir.resolve("signature_statistics_validate_block_command.csv");

        // Track validation progress
        final long startNanos = System.nanoTime();
        long blocksValidated = checkpoint != null ? checkpoint.blocksValidated() : 0;
        long lastValidatedBlockNum = checkpoint != null ? checkpoint.lastValidatedBlockNumber() : -1L;

        // Speed tracking: ratio of consensus-time elapsed to wall-clock elapsed
        long speedCalcBlockTimeMillis = 0; // consensus epoch millis at last speed-calc reset
        long speedCalcRealTimeNanos = System.nanoTime();
        String speedString = "";

        // Flag set in the finally block to prevent the shutdown hook from double-saving.
        final boolean[] completed = {false};
        // Mutable holders for shutdown hook access
        final long[] lastValidatedRef = {lastValidatedBlockNum};
        final long[] blocksValidatedRef = {blocksValidated};

        // Lock to prevent the shutdown hook from saving a checkpoint while commitState() is
        // in progress. Without this, the hook can observe partially-committed validation state
        // (e.g. RunningAccountsState with some mutations from block N+1 applied) while
        // lastValidatedRef still points to block N, causing a supply mismatch on resume.
        final Object commitLock = new Object();

        // Shutdown hook: save checkpoint if interrupted mid-run (e.g. Ctrl-C).
        final Thread shutdownHook = new Thread(
                () -> {
                    synchronized (commitLock) {
                        if (!completed[0]
                                && lastValidatedRef[0] >= 0
                                && chainValidation.getPreviousBlockHash() != null) {
                            System.err.println(
                                    "Shutdown: saving validation checkpoint at block " + lastValidatedRef[0]);
                            try {
                                Files.createDirectories(checkpointDir);
                            } catch (IOException ignored) {
                            }
                            saveCheckpoint(
                                    checkpointDir,
                                    lastValidatedRef[0],
                                    blocksValidatedRef[0],
                                    chainValidation,
                                    validations);
                        }
                    }
                },
                "validate-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // ── Pipeline ─────────────────────────────────────────────────────────
        ExecutorService decompPool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        @SuppressWarnings("unchecked")
        final Future<PreValidatedBlock>[] sentinelHolder = new Future[1];
        sentinelHolder[0] = new CompletableFuture<>();
        final Future<PreValidatedBlock> endOfStream = sentinelHolder[0];

        BlockingQueue<Future<PreValidatedBlock>> blockQueue = new ArrayBlockingQueue<>(prefetch);
        final long effectiveResumeFrom = resumeFrom;
        final List<BlockSource> effectivePendingSources = pendingSources;

        // I/O thread — streams zips sequentially, enqueues decompression futures
        Thread readerThread = new Thread(
                () -> {
                    try {
                        int i = 0;
                        while (i < effectivePendingSources.size()) {
                            BlockSource first = effectivePendingSources.get(i);
                            if (first.isWholeZip()) {
                                // Whole-zip source: stream all entries, discover blocks on the fly
                                try {
                                    readAllZipEntries(
                                            first.filePath(),
                                            effectiveResumeFrom,
                                            parallelValidations,
                                            blockQueue,
                                            decompPool);
                                } catch (IOException e) {
                                    corruptZipCount.incrementAndGet();
                                    System.out.println(Ansi.AUTO.string("@|yellow Warning:|@ error streaming zip: "
                                            + first.filePath().getFileName() + " — " + e.getMessage()));
                                }
                                i++;
                            } else if (!first.isZipEntry()) {
                                // Standalone block file
                                final Path path = first.filePath();
                                final boolean[] flags = BlockZipsUtilities.compressionFlags(first);
                                final boolean isZstd = flags[0];
                                final boolean isGz = flags[1];
                                final long bNum = first.blockNumber();
                                blockQueue.put(decompPool.submit(() -> {
                                    byte[] raw = Files.readAllBytes(path);
                                    BlockUnparsed block =
                                            BlockZipsUtilities.decompressAndPartialParse(raw, isZstd, isGz);
                                    return buildPreValidatedBlock(block, bNum, parallelValidations);
                                }));
                                i++;
                            } else {
                                // Per-entry zip source (e.g. from jimfs tests)
                                Path zipPath = first.filePath();
                                int runEnd = i;
                                Set<String> wanted = new HashSet<>();
                                while (runEnd < effectivePendingSources.size()
                                        && effectivePendingSources.get(runEnd).isZipEntry()
                                        && effectivePendingSources
                                                .get(runEnd)
                                                .filePath()
                                                .equals(zipPath)) {
                                    String en =
                                            effectivePendingSources.get(runEnd).zipEntryName();
                                    if (en.startsWith("/")) en = en.substring(1);
                                    wanted.add(en);
                                    runEnd++;
                                }

                                try {
                                    readZipEntries(zipPath, wanted, parallelValidations, blockQueue, decompPool);
                                } catch (IOException e) {
                                    corruptZipCount.incrementAndGet();
                                    System.out.println(Ansi.AUTO.string(
                                            "@|yellow Warning:|@ error streaming zip (blocks skipped): "
                                                    + zipPath.getFileName() + " — " + e.getMessage()));
                                    for (int k = i; k < runEnd; k++) {
                                        blockQueue.put(CompletableFuture.completedFuture(SKIPPED_BLOCK));
                                    }
                                }
                                i = runEnd;
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("Reader thread error: " + e.getMessage());
                    } finally {
                        try {
                            blockQueue.put(endOfStream);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                "block-reader");
        readerThread.setDaemon(true);
        readerThread.start();

        // ── Validation loop (fail-fast) ──────────────────────────────────────
        String failedValidationName = null;
        String failureMessage = null;
        long failedBlockNumber = -1;
        long skippedBlockCount = 0;

        // Timing accumulators (only populated when --verbose is set)
        final long[] totalQueueTakeNanosRef = {0};
        final long[] totalFutureGetNanosRef = {0};
        final long[] totalCommitNanosRef = {0};
        final Map<String, long[]> perValidationNanos = verbose ? new HashMap<>() : null;
        if (verbose) {
            for (BlockValidation v : sequentialValidations) {
                perValidationNanos.put(v.name(), new long[] {0});
            }
        }

        try (final SignatureStatsCollector statsCollector = new SignatureStatsCollector(statsCsvPath)) {
            try {
                long lastCheckpointSaveMs = System.currentTimeMillis();
                long lastProgressMs = System.currentTimeMillis();

                while (true) {

                    try {
                        long t0 = verbose ? System.nanoTime() : 0;
                        Future<PreValidatedBlock> future = blockQueue.take();
                        if (future == endOfStream) break;
                        if (verbose) totalQueueTakeNanosRef[0] += System.nanoTime() - t0;

                        long t1 = verbose ? System.nanoTime() : 0;
                        PreValidatedBlock preValidated = future.get();
                        if (verbose) totalFutureGetNanosRef[0] += System.nanoTime() - t1;
                        if (preValidated.block() == null) {
                            skippedBlockCount++;
                            continue; // block skipped (corrupt zip mid-stream)
                        }

                        // Check for pre-validation errors (from parallel stage)
                        // Signature errors may be caused by a stale address book — the prefetch
                        // queue may have validated this block before AddressBookUpdateValidation
                        // discovered an address book change in an earlier block. We defer the
                        // error and retry after running sequential validations (which update
                        // the address book).
                        boolean signatureRetryNeeded = false;
                        if (preValidated.preValidationError() != null) {
                            if ("Signatures".equals(preValidated.preValidationName()) && signatureValidation != null) {
                                // Defer — will retry after sequential validations update the address book
                                signatureRetryNeeded = true;
                            } else {
                                failedValidationName = preValidated.preValidationName();
                                failureMessage =
                                        preValidated.preValidationError().getMessage();
                                failedBlockNumber = preValidated.blockNumber();
                                try {
                                    Files.createDirectories(checkpointDir);
                                } catch (IOException ignored) {
                                }
                                saveCheckpoint(
                                        checkpointDir,
                                        lastValidatedRef[0],
                                        blocksValidatedRef[0],
                                        chainValidation,
                                        validations);
                                break;
                            }
                        }

                        BlockUnparsed block = preValidated.block();
                        long blockNum = preValidated.blockNumber();

                        // Phase 1: validate sequential (stateful) validations only,
                        // passing pre-computed data to avoid redundant work on the main thread
                        final PreprocessedData ppd = new PreprocessedData(
                                preValidated.blockHash(), preValidated.blockInstant(), preValidated.recordFileBytes());
                        for (BlockValidation v : sequentialValidations) {
                            try {
                                long vStart = verbose ? System.nanoTime() : 0;
                                v.validate(block, blockNum, ppd);
                                if (verbose) {
                                    perValidationNanos.get(v.name())[0] += System.nanoTime() - vStart;
                                }
                            } catch (ValidationException e) {
                                failedValidationName = v.name();
                                failureMessage = e.getMessage();
                                failedBlockNumber = blockNum;
                                // Save checkpoint before reporting
                                try {
                                    Files.createDirectories(checkpointDir);
                                } catch (IOException ignored) {
                                }
                                saveCheckpoint(
                                        checkpointDir,
                                        lastValidatedRef[0],
                                        blocksValidatedRef[0],
                                        chainValidation,
                                        validations);
                                break;
                            }
                        }
                        if (failedValidationName != null) break;

                        // Retry signature validation with the (now potentially updated) address book
                        if (signatureRetryNeeded) {
                            try {
                                signatureValidation.validate(block, blockNum);
                            } catch (ValidationException e) {
                                failedValidationName = signatureValidation.name();
                                failureMessage = e.getMessage();
                                failedBlockNumber = blockNum;
                                try {
                                    Files.createDirectories(checkpointDir);
                                } catch (IOException ignored) {
                                }
                                saveCheckpoint(
                                        checkpointDir,
                                        lastValidatedRef[0],
                                        blocksValidatedRef[0],
                                        chainValidation,
                                        validations);
                                break;
                            }
                        }

                        // Phase 2: commit all validations (under lock to prevent shutdown hook
                        // from saving a partially-committed state)
                        long commitStart = verbose ? System.nanoTime() : 0;
                        synchronized (commitLock) {
                            for (BlockValidation v : validations) {
                                v.commitState(block, blockNum);
                            }

                            lastValidatedRef[0] = blockNum;
                            blocksValidated++;
                            blocksValidatedRef[0] = blocksValidated;
                        }
                        if (verbose) totalCommitNanosRef[0] += System.nanoTime() - commitStart;

                        // Collect signature stats from the validation
                        if (signatureValidation != null) {
                            SignatureBlockStats blockStats = signatureValidation.popBlockStats(blockNum);
                            if (blockStats != null) {
                                statsCollector.accept(blockStats);
                            }
                        }

                        // Periodic registry sync for crash safety (~every 100 blocks)
                        if (registry != null && blocksValidated % 100 == 0) {
                            registry.sync();
                        }

                        // Periodic checkpoint save
                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastCheckpointSaveMs >= 60_000L) {
                            try {
                                Files.createDirectories(checkpointDir);
                            } catch (IOException ignored) {
                            }
                            saveCheckpoint(checkpointDir, blockNum, blocksValidated, chainValidation, validations);
                            lastCheckpointSaveMs = nowMs;
                        }

                        if (verbose) {
                            final String blockHash = (chainValidation.getPreviousBlockHash() != null)
                                    ? Bytes.wrap(chainValidation.getPreviousBlockHash())
                                            .toHex()
                                    : "null";
                            System.out.println("Block " + blockNum + ": "
                                    + Ansi.AUTO.string("@|green VALID|@") + " (hash: "
                                    + blockHash
                                    + ")");
                        }

                        // Progress every 5 seconds
                        if (nowMs - lastProgressMs >= 5_000L) {
                            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                            long currentPercent = (blocksValidated * 100) / estimatedTotalBlocks;
                            long remainingMillis = PrettyPrint.computeRemainingMilliseconds(
                                    blocksValidated, estimatedTotalBlocks, elapsedMillis);

                            // Compute speed multiplier (consensus-time / wall-clock)
                            BlockHeader parsedHeader = BlockHeader.PROTOBUF.parse(
                                    block.blockItems().getFirst().blockHeaderOrThrow());
                            Timestamp blockTs = parsedHeader.blockTimestampOrThrow();
                            long blockEpochMillis = blockTs.seconds() * 1000L + blockTs.nanos() / 1_000_000L;
                            long currentNanos = System.nanoTime();
                            if (speedCalcBlockTimeMillis == 0) {
                                speedCalcBlockTimeMillis = blockEpochMillis;
                                speedCalcRealTimeNanos = currentNanos;
                                speedString = "";
                            } else {
                                long realElapsedNanos = currentNanos - speedCalcRealTimeNanos;
                                // Reset tracking window every 10 seconds of real time
                                if (realElapsedNanos >= 10_000_000_000L) {
                                    long dataTimeMs = blockEpochMillis - speedCalcBlockTimeMillis;
                                    long realTimeMs = realElapsedNanos / 1_000_000L;
                                    double multiplier = (double) dataTimeMs / (double) realTimeMs;
                                    speedString = String.format(" speed %.1fx", multiplier);
                                    speedCalcBlockTimeMillis = blockEpochMillis;
                                    speedCalcRealTimeNanos = currentNanos;
                                } else if (realElapsedNanos >= 1_000_000_000L) {
                                    long dataTimeMs = blockEpochMillis - speedCalcBlockTimeMillis;
                                    long realTimeMs = realElapsedNanos / 1_000_000L;
                                    double multiplier = (double) dataTimeMs / (double) realTimeMs;
                                    speedString = String.format(" speed %.1fx", multiplier);
                                }
                            }

                            String progressString = "Validated " + blocksValidated + "/~" + estimatedTotalBlocks
                                    + " blocks" + speedString;
                            PrettyPrint.printProgressWithEta(currentPercent, progressString, remainingMillis);
                            lastProgressMs = nowMs;
                        }

                    } catch (Exception e) {
                        failedValidationName = "Block Processing";
                        // Unwrap ExecutionException to surface the actual cause
                        Throwable cause = (e instanceof java.util.concurrent.ExecutionException && e.getCause() != null)
                                ? e.getCause()
                                : e;
                        failureMessage = cause.getMessage() != null ? cause.getMessage() : cause.toString();
                        failedBlockNumber = lastValidatedRef[0] + 1;
                        // Always print for unexpected errors (not just in verbose mode)
                        System.err.println("Unexpected error near block " + failedBlockNumber + ": " + failureMessage);
                        cause.printStackTrace();
                        break;
                    }
                }

                // Stop reader and decompression pool
                readerThread.interrupt();
                decompPool.shutdownNow();

                // ── Finalize all validations (end-of-stream checks) ─────────────
                if (failedValidationName == null && failureMessage == null) {
                    for (BlockValidation v : validations) {
                        try {
                            v.finalize(blocksValidated, lastValidatedRef[0]);
                        } catch (ValidationException e) {
                            failedValidationName = v.name();
                            failureMessage = e.getMessage();
                            failedBlockNumber = -1; // not a per-block error
                            break;
                        }
                    }
                }

                // Fail if blocks were skipped due to corrupt zips
                if (failedValidationName == null && failureMessage == null && skippedBlockCount > 0) {
                    failedValidationName = "Skipped Blocks";
                    failureMessage = skippedBlockCount + " blocks were skipped (corrupt zip files) and could not be"
                            + " validated";
                }

            } finally {
                completed[0] = true;
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM already shutting down
                }
                // Close all validations
                for (BlockValidation v : validations) {
                    v.close();
                }
                // Finalize signature statistics (statsCollector is closed automatically by try-with-resources)
                statsCollector.finalizeDayStats();
                statsCollector.printFinalSummary();
            }
        }

        // Determine overall result
        boolean validationFailed = (failedValidationName != null || failureMessage != null);
        boolean allBlocksValidated =
                (blocksValidated >= estimatedTotalBlocks - BlockZipsUtilities.DEFAULT_BLOCKS_PER_ZIP
                        || blocksValidated == estimatedTotalBlocks);

        // Save checkpoint on both full success and partial completion so the next run
        // can resume from where this one left off (instead of re-validating from block 0).
        if (!validationFailed && lastValidatedRef[0] >= 0 && chainValidation.getPreviousBlockHash() != null) {
            try {
                Files.createDirectories(checkpointDir);
            } catch (IOException ignored) {
            }
            saveCheckpoint(checkpointDir, lastValidatedRef[0], blocksValidated, chainValidation, validations);
            System.out.println(
                    Ansi.AUTO.string("@|yellow Checkpoint saved:|@ " + checkpointDir + "/validateProgress.json"));
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
        if (skippedBlockCount > 0) {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Blocks skipped:|@ " + skippedBlockCount + " (corrupt zip mid-stream)"));
        }
        if (corruptZipCount.get() > 0) {
            System.out.println(Ansi.AUTO.string(
                    "@|yellow Corrupt zips skipped:|@ " + corruptZipCount.get() + " (blocks inside not validated)"));
        }

        if (validationFailed) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,red VALIDATION FAILED|@"));
            if (failedBlockNumber >= 0) {
                System.out.println(Ansi.AUTO.string("@|yellow Block:|@ " + failedBlockNumber));
            }
            if (failedValidationName != null) {
                System.out.println(Ansi.AUTO.string("@|yellow Validation:|@ " + failedValidationName));
            }
            System.out.println(Ansi.AUTO.string("@|yellow Error:|@ " + failureMessage));
            if (lastValidatedRef[0] >= 0) {
                System.out.println(
                        Ansi.AUTO.string("@|yellow Checkpoint saved:|@ " + checkpointDir + "/validateProgress.json"));
            }
        } else {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        }

        long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000L;
        System.out.println(Ansi.AUTO.string("@|yellow Time elapsed:|@ " + elapsedSeconds + " seconds"));

        // Print timing breakdown when --verbose is enabled
        if (verbose && blocksValidated > 0) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,cyan TIMING BREAKDOWN|@"));
            System.out.printf(
                    "  Queue take:          %,d ms (%.1f us/block)%n",
                    totalQueueTakeNanosRef[0] / 1_000_000L, totalQueueTakeNanosRef[0] / 1000.0 / blocksValidated);
            System.out.printf(
                    "  Future get:          %,d ms (%.1f us/block)%n",
                    totalFutureGetNanosRef[0] / 1_000_000L, totalFutureGetNanosRef[0] / 1000.0 / blocksValidated);
            for (BlockValidation v : sequentialValidations) {
                long[] nanos = perValidationNanos.get(v.name());
                if (nanos != null) {
                    System.out.printf(
                            "  %-22s %,d ms (%.1f us/block)%n",
                            v.name() + ":", nanos[0] / 1_000_000L, nanos[0] / 1000.0 / blocksValidated);
                }
            }
            System.out.printf(
                    "  Commit:              %,d ms (%.1f us/block)%n",
                    totalCommitNanosRef[0] / 1_000_000L, totalCommitNanosRef[0] / 1000.0 / blocksValidated);
        }
    }

    /**
     * Loads bundled balance checkpoint files from jar resources.
     *
     * @param validator the validator to load checkpoints into
     * @throws IOException if reading a resource stream fails
     */
    private void loadBundledCheckpoints(final BalanceCheckpointValidator validator) throws IOException {
        final String[] bundledFiles = {"accountBalances_91019204.pb.gz"};
        for (String filename : bundledFiles) {
            try (InputStream stream = getClass().getResourceAsStream("/metadata/" + filename)) {
                if (stream != null) {
                    final String blockStr =
                            filename.replace("accountBalances_", "").replace(".pb.gz", "");
                    final long blockNumber = Long.parseLong(blockStr);
                    validator.loadFromGzippedStream(stream, blockNumber);
                    System.out.println(Ansi.AUTO.string("@|yellow Loaded bundled checkpoint:|@ block " + blockNumber));
                }
            }
        }
    }

    /**
     * Reads wanted entries from a zip file, submitting decompression+parse futures to the queue. Uses sequential
     * {@link ZipInputStream} for speed, falling back to random-access {@link ZipFile} if ZipInputStream fails
     * (e.g. STORED entries with data descriptors written by ZipFileSystem on resume).
     */
    private static void readZipEntries(
            Path zipPath,
            Set<String> wanted,
            List<BlockValidation> parallelValidations,
            BlockingQueue<Future<PreValidatedBlock>> blockQueue,
            ExecutorService decompPool)
            throws IOException, InterruptedException {
        try {
            readZipEntriesViaStream(zipPath, wanted, parallelValidations, blockQueue, decompPool);
        } catch (IOException streamErr) {
            // ZipInputStream rejects STORED entries with data descriptors (General Purpose Bit 3).
            // Fall back to ZipFile which uses the Central Directory and handles any valid zip.
            readZipEntriesViaRandomAccess(zipPath, wanted, parallelValidations, blockQueue, decompPool);
        }
    }

    /**
     * Streams ALL block entries from a zip archive (lazy discovery — no pre-known entry list).
     * Entries with block numbers {@code <= skipBlocksUpTo} are skipped (for resume support).
     */
    private static void readAllZipEntries(
            Path zipPath,
            long skipBlocksUpTo,
            List<BlockValidation> parallelValidations,
            BlockingQueue<Future<PreValidatedBlock>> blockQueue,
            ExecutorService decompPool)
            throws IOException, InterruptedException {
        try (ZipInputStream zis =
                new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipPath), ZIP_READ_BUFFER))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                final String entryName = entry.getName();
                final String fileName = entryName.substring(entryName.lastIndexOf('/') + 1);
                final long bNum = BlockZipsUtilities.extractBlockNumber(fileName);
                if (bNum < 0 || entry.isDirectory()) {
                    zis.closeEntry();
                    continue;
                }
                if (bNum <= skipBlocksUpTo) {
                    zis.closeEntry();
                    continue;
                }
                final boolean[] flags = BlockZipsUtilities.compressionFlags(entryName);
                final boolean isZstd = flags[0];
                final boolean isGz = flags[1];
                final byte[] raw = zis.readAllBytes();
                blockQueue.put(decompPool.submit(() -> {
                    BlockUnparsed block = BlockZipsUtilities.decompressAndPartialParse(raw, isZstd, isGz);
                    return buildPreValidatedBlock(block, bNum, parallelValidations);
                }));
            }
        } catch (IOException streamErr) {
            // ZipInputStream rejects STORED entries with data descriptors (General Purpose Bit 3).
            // Fall back to ZipFile which reads the Central Directory and handles any valid zip.
            try (ZipFile zf = new ZipFile(zipPath.toFile())) {
                java.util.Enumeration<? extends ZipEntry> entries = zf.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry fallbackEntry = entries.nextElement();
                    if (fallbackEntry.isDirectory()) continue;
                    final String entryName2 = fallbackEntry.getName();
                    final String fileName2 = entryName2.substring(entryName2.lastIndexOf('/') + 1);
                    final long bNum = BlockZipsUtilities.extractBlockNumber(fileName2);
                    if (bNum < 0 || bNum <= skipBlocksUpTo) continue;
                    final boolean[] flags = BlockZipsUtilities.compressionFlags(entryName2);
                    final boolean isZstd = flags[0];
                    final boolean isGz = flags[1];
                    final byte[] raw;
                    try (var entryStream = zf.getInputStream(fallbackEntry)) {
                        raw = entryStream.readAllBytes();
                    }
                    blockQueue.put(decompPool.submit(() -> {
                        BlockUnparsed block = BlockZipsUtilities.decompressAndPartialParse(raw, isZstd, isGz);
                        return buildPreValidatedBlock(block, bNum, parallelValidations);
                    }));
                }
            }
        }
    }

    /** Reads zip entries sequentially via {@link ZipInputStream} (fast, but strict about data descriptors). */
    private static void readZipEntriesViaStream(
            Path zipPath,
            Set<String> wanted,
            List<BlockValidation> parallelValidations,
            BlockingQueue<Future<PreValidatedBlock>> blockQueue,
            ExecutorService decompPool)
            throws IOException, InterruptedException {
        try (ZipInputStream zis =
                new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipPath), ZIP_READ_BUFFER))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                final String entryName = entry.getName();
                if (!wanted.contains(entryName)) {
                    zis.closeEntry();
                    continue;
                }
                final boolean[] flags = BlockZipsUtilities.compressionFlags(entryName);
                final boolean isZstd = flags[0];
                final boolean isGz = flags[1];
                final byte[] raw = zis.readAllBytes();
                final String fileName = entryName.substring(entryName.lastIndexOf('/') + 1);
                final long bNum = BlockZipsUtilities.extractBlockNumber(fileName);
                blockQueue.put(decompPool.submit(() -> {
                    BlockUnparsed block = BlockZipsUtilities.decompressAndPartialParse(raw, isZstd, isGz);
                    return buildPreValidatedBlock(block, bNum, parallelValidations);
                }));
            }
        }
    }

    /** Reads zip entries via random-access {@link ZipFile} (robust, handles any valid zip format). */
    private static void readZipEntriesViaRandomAccess(
            Path zipPath,
            Set<String> wanted,
            List<BlockValidation> parallelValidations,
            BlockingQueue<Future<PreValidatedBlock>> blockQueue,
            ExecutorService decompPool)
            throws IOException, InterruptedException {
        try (ZipFile zf = new ZipFile(zipPath.toFile())) {
            for (String entryName : wanted) {
                ZipEntry entry = zf.getEntry(entryName);
                if (entry == null) continue;
                final boolean[] flags = BlockZipsUtilities.compressionFlags(entryName);
                final boolean isZstd = flags[0];
                final boolean isGz = flags[1];
                final byte[] raw;
                try (var entryStream = zf.getInputStream(entry)) {
                    raw = entryStream.readAllBytes();
                }
                final String fileName = entryName.substring(entryName.lastIndexOf('/') + 1);
                final long bNum = BlockZipsUtilities.extractBlockNumber(fileName);
                blockQueue.put(decompPool.submit(() -> {
                    BlockUnparsed block = BlockZipsUtilities.decompressAndPartialParse(raw, isZstd, isGz);
                    return buildPreValidatedBlock(block, bNum, parallelValidations);
                }));
            }
        }
    }

    /**
     * Runs stateless validations that can safely execute in parallel on worker threads.
     *
     * @param checks the pre-built list of stateless validations to run
     * @param block the shallow-parsed block to validate
     * @param blockNumber the block number
     * @return a two-element array {@code [validationName, exception]} on failure, or null if all passed
     */
    private static Object[] runParallelValidations(
            List<BlockValidation> checks, BlockUnparsed block, long blockNumber) {
        for (BlockValidation v : checks) {
            try {
                v.validate(block, blockNumber);
            } catch (Exception e) {
                return new Object[] {v.name(), e};
            }
        }
        return null;
    }

    /**
     * Runs parallel validations and preprocessing, then wraps the results into a {@link PreValidatedBlock}.
     */
    private static PreValidatedBlock buildPreValidatedBlock(
            final BlockUnparsed block, final long blockNumber, final List<BlockValidation> parallelValidations) {
        Object[] err = runParallelValidations(parallelValidations, block, blockNumber);
        PreprocessedData ppd = ParallelBlockPreprocessor.preprocess(block);
        return new PreValidatedBlock(
                block,
                blockNumber,
                err != null ? (String) err[0] : null,
                err != null ? (Exception) err[1] : null,
                ppd.blockHash(),
                ppd.blockInstant(),
                ppd.recordFileBytes());
    }
}
