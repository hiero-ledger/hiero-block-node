// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
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
import java.util.zip.ZipInputStream;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.BlockSource;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.ParsedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.blocks.validation.BlockChainValidation;
import org.hiero.block.tools.blocks.validation.BlockStructureValidation;
import org.hiero.block.tools.blocks.validation.BlockValidation;
import org.hiero.block.tools.blocks.validation.HashRegistryValidation;
import org.hiero.block.tools.blocks.validation.HbarSupplyValidation;
import org.hiero.block.tools.blocks.validation.HistoricalBlockTreeValidation;
import org.hiero.block.tools.blocks.validation.RequiredItemsValidation;
import org.hiero.block.tools.blocks.validation.SignatureValidation;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Validates a wrapped block stream by checking:
 * <ul>
 *   <li>Hash chain continuity - each block's previousBlockRootHash matches computed hash of previous block</li>
 *   <li>First block has the empty-tree hash for previous hash (genesis)</li>
 *   <li>Signature validation - at least 1/3 + 1 of address book nodes must sign</li>
 *   <li>Binary state files produced by {@code ToWrappedBlocksCommand}:
 *     {@code blockStreamBlockHashes.bin}, {@code streamingMerkleTree.bin},
 *     {@code completeMerkleTree.bin}, and {@code jumpstart.bin}</li>
 * </ul>
 *
 * <p>This command works with both:</p>
 * <ul>
 *   <li>Individual block files (*.blk, *.blk.gz, *.blk.zstd)</li>
 *   <li>Hierarchical directory structures produced by {@code ToWrappedBlocksCommand} and {@code BlockWriter}</li>
 *   <li>Zip archives containing multiple blocks</li>
 * </ul>
 *
 * <p>When validating output from {@code ToWrappedBlocksCommand}, you can simply pass the output directory
 * as the only parameter. The command will automatically find the {@code addressBookHistory.json} file
 * in that directory if not explicitly specified, and will validate all four binary state files if present.</p>
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
    private File[] files;

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

        // Auto-detect addressBookHistory.json if not explicitly provided
        if (addressBookFile == null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    Path potentialAddressBook = file.toPath().resolve("addressBookHistory.json");
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
        AddressBookRegistry addressBookRegistry = null;
        if (addressBookFile != null && Files.exists(addressBookFile)) {
            addressBookRegistry = new AddressBookRegistry(addressBookFile);
            System.out.println(Ansi.AUTO.string("@|yellow Loaded address book from:|@ " + addressBookFile));
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
                .filter(File::isDirectory)
                .map(f -> f.toPath().resolve("validateCheckpoint"))
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
        for (File file : files) {
            if (file.isDirectory()) {
                Path dir = file.toPath();
                if (Files.exists(dir.resolve("blockStreamBlockHashes.bin"))) {
                    hashRegistryPath = dir.resolve("blockStreamBlockHashes.bin");
                    streamingMerkleTreePath = dir.resolve("streamingMerkleTree.bin");
                    completeMerkleTreePath = dir.resolve("completeMerkleTree.bin");
                    jumpstartPath = dir.resolve("jumpstart.bin");
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

        // Check for missing companion state files upfront
        long stateFileErrors = 0;
        if (hasStateFiles) {
            if (!Files.exists(streamingMerkleTreePath)) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ streamingMerkleTree.bin not found alongside blockStreamBlockHashes.bin"));
                stateFileErrors++;
            }
            if (!Files.exists(completeMerkleTreePath)) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ completeMerkleTree.bin not found alongside blockStreamBlockHashes.bin"));
                stateFileErrors++;
            }
            if (!Files.exists(jumpstartPath)) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ jumpstart.bin not found alongside blockStreamBlockHashes.bin"));
                stateFileErrors++;
            }
        }

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

        List<BlockValidation> validations = new ArrayList<>();
        validations.add(new RequiredItemsValidation());
        validations.add(new BlockStructureValidation());
        validations.add(chainValidation);
        validations.add(treeValidation);
        validations.add(supplyValidation);
        validations.add(new SignatureValidation(addressBookRegistry));
        if (registry != null) {
            validations.add(new HashRegistryValidation(registry, chainValidation));
        }

        // InMemoryTreeHasher is only used for post-loop state file validation
        final InMemoryTreeHasher freshInMemoryHasher = new InMemoryTreeHasher();

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

        // Rebuild InMemoryTreeHasher by replaying already-validated block hashes from registry
        if (checkpoint != null && registry != null && checkpoint.lastValidatedBlockNumber() >= 0) {
            long firstBlock = sources.getFirst().blockNumber();
            long replayTo = checkpoint.lastValidatedBlockNumber();
            System.out.println(Ansi.AUTO.string("@|yellow Replaying |@" + (replayTo - firstBlock + 1)
                    + " block hashes into in-memory hasher (blocks " + firstBlock + ".." + replayTo + ")"));
            for (long bn = firstBlock; bn <= replayTo; bn++) {
                freshInMemoryHasher.addNodeByHash(registry.getBlockHash(bn));
            }
        }

        // Track validation progress
        final long startNanos = System.nanoTime();
        long blocksValidated = checkpoint != null ? checkpoint.blocksValidated() : 0;
        long lastValidatedBlockNum = checkpoint != null ? checkpoint.lastValidatedBlockNumber() : -1L;

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
        final Future<ParsedBlock>[] sentinelHolder = new Future[1];
        sentinelHolder[0] = new CompletableFuture<>();
        final Future<ParsedBlock> endOfStream = sentinelHolder[0];

        BlockingQueue<Future<ParsedBlock>> blockQueue = new ArrayBlockingQueue<>(prefetch);

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
                                    return new ParsedBlock(
                                            BlockZipsUtilities.decompressAndParse(raw, isZstd, isGz), bNum);
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

                                try (ZipInputStream zis = new ZipInputStream(
                                        new BufferedInputStream(Files.newInputStream(zipPath), ZIP_READ_BUFFER))) {
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
                                        blockQueue.put(decompPool.submit(() -> new ParsedBlock(
                                                BlockZipsUtilities.decompressAndParse(raw, isZstd, isGz), bNum)));
                                    }
                                } catch (IOException e) {
                                    corruptZipCount.incrementAndGet();
                                    System.out.println(Ansi.AUTO.string(
                                            "@|yellow Warning:|@ error streaming zip (blocks skipped): "
                                                    + zipPath.getFileName() + " — " + e.getMessage()));
                                    for (int k = i; k < runEnd; k++) {
                                        blockQueue.put(CompletableFuture.completedFuture(null));
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

        try {
            long lastCheckpointSaveMs = System.currentTimeMillis();
            long lastProgressMs = System.currentTimeMillis();

            for (int i = 0; i < pendingSources.size(); i++) {
                long blockNum = pendingSources.get(i).blockNumber();

                try {
                    Future<ParsedBlock> future = blockQueue.take();
                    if (future == endOfStream) break;

                    ParsedBlock parsed = future.get();
                    if (parsed == null) continue; // block skipped (corrupt zip mid-stream)

                    Block block = parsed.block();

                    // Phase 1: validate all
                    for (BlockValidation v : validations) {
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

                    // Phase 2: commit all
                    for (BlockValidation v : validations) {
                        v.commitState(block, blockNum);
                    }
                    // Also feed the in-memory hasher (not a BlockValidation)
                    freshInMemoryHasher.addNodeByHash(chainValidation.getPreviousBlockHash());

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
                        String progressString = "Validated " + blocksValidated + "/" + sources.size() + " blocks";
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

            // ── Post-loop: validate binary state files ───────────────────────
            StreamingHasher freshStreamingHasher = treeValidation.getStreamingHasher();

            // Validate blockStreamBlockHashes.bin highest stored block number
            if (registry != null) {
                long expectedHighest = sources.getLast().blockNumber();
                if (registry.highestBlockNumberStored() != expectedHighest) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ blockStreamBlockHashes.bin highest stored block "
                                    + registry.highestBlockNumberStored() + " != expected " + expectedHighest));
                    stateFileErrors++;
                }
            }

            // Validate streamingMerkleTree.bin
            if (streamingMerkleTreePath != null && Files.exists(streamingMerkleTreePath)) {
                StreamingHasher loadedStreaming = new StreamingHasher();
                try {
                    loadedStreaming.load(streamingMerkleTreePath);
                    long expectedLeaves = sources.size();
                    if (loadedStreaming.leafCount() != expectedLeaves) {
                        PrettyPrint.clearProgress();
                        System.out.println(
                                Ansi.AUTO.string("@|red State file error:|@ streamingMerkleTree.bin leaf count "
                                        + loadedStreaming.leafCount() + " != expected " + expectedLeaves));
                        stateFileErrors++;
                    }
                    if (!Arrays.equals(loadedStreaming.computeRootHash(), freshStreamingHasher.computeRootHash())) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red State file error:|@ streamingMerkleTree.bin root hash mismatch"));
                        stateFileErrors++;
                    }
                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ Failed to load streamingMerkleTree.bin: " + e.getMessage()));
                    stateFileErrors++;
                }
            }

            // Validate completeMerkleTree.bin
            if (completeMerkleTreePath != null && Files.exists(completeMerkleTreePath)) {
                InMemoryTreeHasher loadedInMemory = new InMemoryTreeHasher();
                try {
                    loadedInMemory.load(completeMerkleTreePath);
                    long expectedLeaves = sources.size();
                    if (loadedInMemory.leafCount() != expectedLeaves) {
                        PrettyPrint.clearProgress();
                        System.out.println(
                                Ansi.AUTO.string("@|red State file error:|@ completeMerkleTree.bin leaf count "
                                        + loadedInMemory.leafCount() + " != expected " + expectedLeaves));
                        stateFileErrors++;
                    }
                    if (!Arrays.equals(loadedInMemory.computeRootHash(), freshInMemoryHasher.computeRootHash())) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red State file error:|@ completeMerkleTree.bin root hash mismatch"));
                        stateFileErrors++;
                    }
                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ Failed to load completeMerkleTree.bin: " + e.getMessage()));
                    stateFileErrors++;
                }
            }

            // Validate jumpstart.bin
            if (jumpstartPath != null && Files.exists(jumpstartPath)) {
                try (DataInputStream din = new DataInputStream(Files.newInputStream(jumpstartPath))) {
                    long jBlockNum = din.readLong();
                    byte[] jHash = new byte[48];
                    din.readFully(jHash);
                    long jLeafCount = din.readLong();
                    int jHashCount = din.readInt();
                    List<byte[]> jHashes = new ArrayList<>();
                    for (int i = 0; i < jHashCount; i++) {
                        byte[] h = new byte[48];
                        din.readFully(h);
                        jHashes.add(h);
                    }
                    long expectedBlockNum = sources.getLast().blockNumber();
                    if (jBlockNum != expectedBlockNum) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string("@|red State file error:|@ jumpstart.bin block number "
                                + jBlockNum + " != expected " + expectedBlockNum));
                        stateFileErrors++;
                    }
                    if (registry != null) {
                        byte[] registryHash = registry.getBlockHash(jBlockNum);
                        if (!Arrays.equals(jHash, registryHash)) {
                            PrettyPrint.clearProgress();
                            System.out.println(Ansi.AUTO.string(
                                    "@|red State file error:|@ jumpstart.bin block hash does not match registry"));
                            stateFileErrors++;
                        }
                    }
                    if (jLeafCount != freshStreamingHasher.leafCount()) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string("@|red State file error:|@ jumpstart.bin leaf count "
                                + jLeafCount + " != expected " + freshStreamingHasher.leafCount()));
                        stateFileErrors++;
                    }
                    StreamingHasher jumpstartHasher = new StreamingHasher(jHashes);
                    if (!Arrays.equals(jumpstartHasher.computeRootHash(), freshStreamingHasher.computeRootHash())) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red State file error:|@ jumpstart.bin streaming tree root mismatch"));
                        stateFileErrors++;
                    }
                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ Failed to read jumpstart.bin: " + e.getMessage()));
                    stateFileErrors++;
                }
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
        if (!validationFailed && stateFileErrors == 0 && allBlocksValidated) {
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
        if (corruptZipCount.get() > 0) {
            System.out.println(Ansi.AUTO.string(
                    "@|yellow Corrupt zips skipped:|@ " + corruptZipCount.get() + " (blocks inside not validated)"));
        }
        if (stateFileErrors > 0) {
            System.out.println(Ansi.AUTO.string("@|yellow State file errors:|@ " + stateFileErrors));
        }

        if (validationFailed) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,red VALIDATION FAILED|@"));
            System.out.println(Ansi.AUTO.string("@|yellow Block:|@ " + failedBlockNumber));
            if (failedValidationName != null) {
                System.out.println(Ansi.AUTO.string("@|yellow Validation:|@ " + failedValidationName));
            }
            System.out.println(Ansi.AUTO.string("@|yellow Error:|@ " + failureMessage));
            System.out.println(
                    Ansi.AUTO.string("@|yellow Checkpoint saved:|@ " + checkpointDir + "/validateProgress.json"));
        } else if (stateFileErrors > 0) {
            System.out.println();
            System.out.println(
                    Ansi.AUTO.string("@|bold,red VALIDATION FAILED|@ - " + stateFileErrors + " state file errors"));
        } else {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        }

        long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000L;
        System.out.println(Ansi.AUTO.string("@|yellow Time elapsed:|@ " + elapsedSeconds + " seconds"));
    }
}
