// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.BlockSource;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.PreValidatedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.validation.BalanceCheckpointValidation;
import org.hiero.block.tools.blocks.validation.BlockChainValidation;
import org.hiero.block.tools.blocks.validation.BlockStructureValidation;
import org.hiero.block.tools.blocks.validation.BlockValidation;
import org.hiero.block.tools.blocks.validation.CompleteMerkleTreeValidation;
import org.hiero.block.tools.blocks.validation.HashRegistryValidation;
import org.hiero.block.tools.blocks.validation.HbarSupplyValidation;
import org.hiero.block.tools.blocks.validation.HistoricalBlockTreeValidation;
import org.hiero.block.tools.blocks.validation.JumpstartValidation;
import org.hiero.block.tools.blocks.validation.RequiredItemsValidation;
import org.hiero.block.tools.blocks.validation.SignatureValidation;
import org.hiero.block.tools.blocks.validation.StreamingMerkleTreeValidation;
import org.hiero.block.tools.blocks.wrapped.BalanceCheckpointValidator;
import org.hiero.block.tools.days.model.AddressBookRegistry;
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
    private int prefetch = 512;

    @Option(
            names = {"--skip-signatures"},
            description = "Skip signature validation (only check hash chain and state)")
    private boolean skipSignatures = false;

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
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No address book found. Provide --address-book or place "
                    + "addressBookHistory.json in the input directory."));
            return;
        }

        // Find all block sources
        List<BlockSource> sources = BlockZipsUtilities.findBlockSources(files, corruptZipCount);
        if (sources.isEmpty()) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No block files found"));
            return;
        }

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

        final long resumeFrom = checkpoint != null ? checkpoint.lastValidatedBlockNumber() : -1L;
        final List<BlockSource> pendingSources = (checkpoint == null)
                ? sources
                : sources.stream().filter(s -> s.blockNumber() > resumeFrom).toList();

        // Detect binary state files in any input directory
        Path hashRegistryPath = null;
        Path streamingMerkleTreePath = null;
        Path completeMerkleTreePath = null;
        Path jumpstartPath = null;
        for (Path file : files) {
            if (Files.isDirectory(file)) {
                if (Files.exists(file.resolve("blockStreamBlockHashes.bin"))) {
                    hashRegistryPath = file.resolve("blockStreamBlockHashes.bin");
                    streamingMerkleTreePath = file.resolve("streamingMerkleTree.bin");
                    completeMerkleTreePath = file.resolve("completeMerkleTree.bin");
                    jumpstartPath = file.resolve("jumpstart.bin");
                }
            }
        }
        final boolean hasStateFiles = (hashRegistryPath != null);

        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   BLOCK STREAM VALIDATION|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow Total blocks to validate:|@ " + sources.size()));
        System.out.println(Ansi.AUTO.string("@|yellow Block range:|@ "
                + sources.getFirst().blockNumber() + " - " + sources.getLast().blockNumber()));
        System.out.println(Ansi.AUTO.string("@|yellow Threads:|@ " + threads + "  prefetch: " + prefetch));
        if (hasStateFiles) {
            System.out.println(Ansi.AUTO.string("@|yellow State files found:|@ blockStreamBlockHashes.bin, "
                    + "streamingMerkleTree.bin, completeMerkleTree.bin, jumpstart.bin"));
        }
        if (checkpoint != null) {
            System.out.println(Ansi.AUTO.string("@|yellow Pending blocks:|@ " + pendingSources.size()
                    + " (resuming after block " + checkpoint.lastValidatedBlockNumber() + ")"));
        }
        System.out.println();

        // Check for gaps in block numbers (across the full dataset)
        long expectedBlockNumber = sources.getFirst().blockNumber();
        for (BlockSource source : sources) {
            if (source.blockNumber() != expectedBlockNumber) {
                System.out.println(Ansi.AUTO.string("@|red Gap detected:|@ Expected block " + expectedBlockNumber
                        + " but found " + source.blockNumber()));
            }
            expectedBlockNumber = source.blockNumber() + 1;
        }

        // ── Build validation list ────────────────────────────────────────────
        BlockStreamBlockHashRegistry registry =
                hasStateFiles ? new BlockStreamBlockHashRegistry(hashRegistryPath) : null;

        BlockChainValidation chainValidation = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chainValidation);
        HbarSupplyValidation supplyValidation = new HbarSupplyValidation();
        CompleteMerkleTreeValidation completeMerkleTreeValidation =
                hasStateFiles ? new CompleteMerkleTreeValidation(completeMerkleTreePath, chainValidation) : null;

        // Parallel validations (stateless, run in decompPool threads)
        final AddressBookRegistry abRegistry = addressBookRegistry;
        List<BlockValidation> parallelValidations = new ArrayList<>();
        parallelValidations.add(new RequiredItemsValidation());
        parallelValidations.add(new BlockStructureValidation());
        if (!skipSignatures) {
            parallelValidations.add(new SignatureValidation(abRegistry));
        } else {
            System.out.println(Ansi.AUTO.string("@|yellow Skipping:|@ Signature validation (--skip-signatures)"));
        }

        // Sequential validations (stateful, run on main thread)
        List<BlockValidation> sequentialValidations = new ArrayList<>();
        sequentialValidations.add(chainValidation);
        sequentialValidations.add(treeValidation);
        sequentialValidations.add(supplyValidation);
        if (registry != null) {
            sequentialValidations.add(new HashRegistryValidation(registry, chainValidation));
        }
        if (hasStateFiles) {
            sequentialValidations.add(completeMerkleTreeValidation);
            sequentialValidations.add(new StreamingMerkleTreeValidation(streamingMerkleTreePath, treeValidation));
            sequentialValidations.add(new JumpstartValidation(jumpstartPath, treeValidation, registry));
        }

        // Balance checkpoint validation (genesis-requiring, optional)
        final long firstBlockNumber = sources.getFirst().blockNumber();
        if (firstBlockNumber == 0 && validateBalances) {
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
        if (firstBlockNumber != 0 && checkpoint == null) {
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
                        + " (requires genesis start, first block is " + firstBlockNumber + ")"));
            }
        }

        // Restore validation state from checkpoint
        if (checkpoint != null) {
            try {
                Files.createDirectories(checkpointDir);
            } catch (IOException ignored) {
            }
            chainValidation.setPreviousBlockHash(checkpoint.previousBlockHash());
            for (BlockValidation v : validations) {
                try {
                    v.load(checkpointDir);
                } catch (Exception e) {
                    System.err.println("Warning: could not load " + v.name() + " state: " + e.getMessage());
                }
            }
            if (treeValidation.getStreamingHasher().leafCount() > 0) {
                System.out.println(Ansi.AUTO.string("@|yellow Restored streaming hasher:|@ leafCount = "
                        + treeValidation.getStreamingHasher().leafCount()));
            }
        }

        // Rebuild CompleteMerkleTreeValidation by replaying already-validated block hashes from registry
        if (checkpoint != null && registry != null && checkpoint.lastValidatedBlockNumber() >= 0) {
            long firstBlock = sources.getFirst().blockNumber();
            long replayTo = checkpoint.lastValidatedBlockNumber();
            System.out.println(Ansi.AUTO.string("@|yellow Replaying |@" + (replayTo - firstBlock + 1)
                    + " block hashes into in-memory hasher (blocks " + firstBlock + ".." + replayTo + ")"));
            completeMerkleTreeValidation.replayFromRegistry(registry, firstBlock, replayTo);
        }

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

        // Shutdown hook: save checkpoint if interrupted mid-run (e.g. Ctrl-C).
        final Thread shutdownHook = new Thread(
                () -> {
                    if (!completed[0] && lastValidatedRef[0] >= 0 && chainValidation.getPreviousBlockHash() != null) {
                        System.err.println("Shutdown: saving validation checkpoint at block " + lastValidatedRef[0]);
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

        // I/O thread — streams zips sequentially, enqueues decompression futures
        Thread readerThread = new Thread(
                () -> {
                    try {
                        int i = 0;
                        while (i < pendingSources.size()) {
                            BlockSource first = pendingSources.get(i);
                            if (!first.isZipEntry()) {
                                final Path path = first.filePath();
                                final boolean[] flags = BlockZipsUtilities.compressionFlags(first);
                                final boolean isZstd = flags[0];
                                final boolean isGz = flags[1];
                                final long bNum = first.blockNumber();
                                blockQueue.put(decompPool.submit(() -> {
                                    byte[] raw = Files.readAllBytes(path);
                                    Block block = BlockZipsUtilities.decompressAndParse(raw, isZstd, isGz);
                                    Object[] err = runParallelValidations(abRegistry, block, bNum, skipSignatures);
                                    return err != null
                                            ? new PreValidatedBlock(block, bNum, (String) err[0], (Exception) err[1])
                                            : new PreValidatedBlock(block, bNum, null, null);
                                }));
                                i++;
                            } else {
                                Path zipPath = first.filePath();
                                int runEnd = i;
                                Set<String> wanted = new HashSet<>();
                                while (runEnd < pendingSources.size()
                                        && pendingSources.get(runEnd).isZipEntry()
                                        && pendingSources.get(runEnd).filePath().equals(zipPath)) {
                                    String en = pendingSources.get(runEnd).zipEntryName();
                                    if (en.startsWith("/")) en = en.substring(1);
                                    wanted.add(en);
                                    runEnd++;
                                }

                                try {
                                    readZipEntries(zipPath, wanted, abRegistry, skipSignatures, blockQueue, decompPool);
                                } catch (IOException e) {
                                    corruptZipCount.incrementAndGet();
                                    System.out.println(Ansi.AUTO.string(
                                            "@|yellow Warning:|@ error streaming zip (blocks skipped): "
                                                    + zipPath.getFileName() + " — " + e.getMessage()));
                                    for (int k = i; k < runEnd; k++) {
                                        blockQueue.put(CompletableFuture.completedFuture(
                                                new PreValidatedBlock(null, -1, null, null)));
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

        try {
            long lastCheckpointSaveMs = System.currentTimeMillis();
            long lastProgressMs = System.currentTimeMillis();

            for (int i = 0; i < pendingSources.size(); i++) {
                long blockNum = pendingSources.get(i).blockNumber();

                try {
                    Future<PreValidatedBlock> future = blockQueue.take();
                    if (future == endOfStream) break;

                    PreValidatedBlock preValidated = future.get();
                    if (preValidated.block() == null) {
                        skippedBlockCount++;
                        continue; // block skipped (corrupt zip mid-stream)
                    }

                    // Check for pre-validation errors (from parallel stage)
                    if (preValidated.preValidationError() != null) {
                        failedValidationName = preValidated.preValidationName();
                        failureMessage = preValidated.preValidationError().getMessage();
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

                    Block block = preValidated.block();

                    // Phase 1: validate sequential (stateful) validations only
                    for (BlockValidation v : sequentialValidations) {
                        try {
                            v.validate(block, blockNum);
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

                    // Phase 2: commit all validations
                    for (BlockValidation v : validations) {
                        v.commitState(block, blockNum);
                    }

                    lastValidatedRef[0] = blockNum;
                    blocksValidated++;
                    blocksValidatedRef[0] = blocksValidated;

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
                        System.out.println("Block " + blockNum + ": "
                                + Ansi.AUTO.string("@|green VALID|@") + " (hash: "
                                + Bytes.wrap(chainValidation.getPreviousBlockHash())
                                        .toHex()
                                        .substring(0, 8)
                                + ")");
                    }

                    // Progress every 5 seconds
                    if (nowMs - lastProgressMs >= 5_000L || i == pendingSources.size() - 1) {
                        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                        long currentPercent = (blocksValidated * 100) / sources.size();
                        long remainingMillis = PrettyPrint.computeRemainingMilliseconds(
                                blocksValidated, sources.size(), elapsedMillis);

                        // Compute speed multiplier (consensus-time / wall-clock)
                        Timestamp blockTs =
                                block.items().getFirst().blockHeader().blockTimestampOrElse(null);
                        if (blockTs != null) {
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
                                    if (realTimeMs > 0) {
                                        double multiplier = (double) dataTimeMs / (double) realTimeMs;
                                        speedString = String.format(" speed %.1fx", multiplier);
                                    }
                                    speedCalcBlockTimeMillis = blockEpochMillis;
                                    speedCalcRealTimeNanos = currentNanos;
                                } else if (realElapsedNanos >= 1_000_000_000L) {
                                    long dataTimeMs = blockEpochMillis - speedCalcBlockTimeMillis;
                                    long realTimeMs = realElapsedNanos / 1_000_000L;
                                    if (realTimeMs > 0) {
                                        double multiplier = (double) dataTimeMs / (double) realTimeMs;
                                        speedString = String.format(" speed %.1fx", multiplier);
                                    }
                                }
                            }
                        }

                        String progressString =
                                "Validated " + blocksValidated + "/" + sources.size() + " blocks" + speedString;
                        PrettyPrint.printProgressWithEta(currentPercent, progressString, remainingMillis);
                        lastProgressMs = nowMs;
                    }

                } catch (Exception e) {
                    failureMessage = e.getMessage();
                    failedBlockNumber = blockNum;
                    if (verbose) {
                        e.printStackTrace();
                    }
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
                failureMessage =
                        skippedBlockCount + " blocks were skipped (corrupt zip files) and could not be" + " validated";
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
        }

        // Determine overall result
        boolean validationFailed = (failedValidationName != null || failureMessage != null);
        boolean allBlocksValidated = (blocksValidated == sources.size());

        // On full success: remove checkpoint (validation is complete).
        // On partial/error: save a final checkpoint for next resume.
        if (!validationFailed && allBlocksValidated) {
            try {
                // Delete all checkpoint files
                if (Files.isDirectory(checkpointDir)) {
                    try (Stream<Path> cpFiles = Files.list(checkpointDir)) {
                        cpFiles.forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                            }
                        });
                    }
                    Files.deleteIfExists(checkpointDir);
                }
                System.out.println(Ansi.AUTO.string("@|yellow Checkpoint deleted|@ (validation complete)"));
            } catch (IOException e) {
                System.err.println("Warning: could not delete checkpoint: " + e.getMessage());
            }
        } else if (!validationFailed && lastValidatedRef[0] >= 0 && chainValidation.getPreviousBlockHash() != null) {
            // Partial completion (state file errors only) — save checkpoint
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
            AddressBookRegistry abRegistry,
            boolean skipSignatures,
            BlockingQueue<Future<PreValidatedBlock>> blockQueue,
            ExecutorService decompPool)
            throws IOException, InterruptedException {
        try {
            readZipEntriesViaStream(zipPath, wanted, abRegistry, skipSignatures, blockQueue, decompPool);
        } catch (IOException streamErr) {
            // ZipInputStream rejects STORED entries with data descriptors (General Purpose Bit 3).
            // Fall back to ZipFile which uses the Central Directory and handles any valid zip.
            readZipEntriesViaRandomAccess(zipPath, wanted, abRegistry, skipSignatures, blockQueue, decompPool);
        }
    }

    /** Reads zip entries sequentially via {@link ZipInputStream} (fast, but strict about data descriptors). */
    private static void readZipEntriesViaStream(
            Path zipPath,
            Set<String> wanted,
            AddressBookRegistry abRegistry,
            boolean skipSignatures,
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
                    Block block = BlockZipsUtilities.decompressAndParse(raw, isZstd, isGz);
                    Object[] err = runParallelValidations(abRegistry, block, bNum, skipSignatures);
                    return err != null
                            ? new PreValidatedBlock(block, bNum, (String) err[0], (Exception) err[1])
                            : new PreValidatedBlock(block, bNum, null, null);
                }));
            }
        }
    }

    /** Reads zip entries via random-access {@link ZipFile} (robust, handles any valid zip format). */
    private static void readZipEntriesViaRandomAccess(
            Path zipPath,
            Set<String> wanted,
            AddressBookRegistry abRegistry,
            boolean skipSignatures,
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
                    Block block = BlockZipsUtilities.decompressAndParse(raw, isZstd, isGz);
                    Object[] err = runParallelValidations(abRegistry, block, bNum, skipSignatures);
                    return err != null
                            ? new PreValidatedBlock(block, bNum, (String) err[0], (Exception) err[1])
                            : new PreValidatedBlock(block, bNum, null, null);
                }));
            }
        }
    }

    /**
     * Runs stateless validations that can safely execute in parallel on worker threads. Creates fresh validation
     * instances per call to make thread safety obvious (construction is cheap).
     *
     * @param addressBookRegistry the address book registry for signature validation
     * @param block the parsed block to validate
     * @param blockNumber the block number
     * @param skipSignatures whether to skip signature validation
     * @return a two-element array {@code [validationName, exception]} on failure, or null if all passed
     */
    private static Object[] runParallelValidations(
            AddressBookRegistry addressBookRegistry, Block block, long blockNumber, boolean skipSignatures) {
        List<BlockValidation> checks = new ArrayList<>();
        checks.add(new RequiredItemsValidation());
        checks.add(new BlockStructureValidation());
        if (!skipSignatures) {
            checks.add(new SignatureValidation(addressBookRegistry));
        }
        for (BlockValidation v : checks) {
            try {
                v.validate(block, blockNumber);
            } catch (Exception e) {
                return new Object[] {v.name(), e};
            }
        }
        return null;
    }
}
