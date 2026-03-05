// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.github.luben.zstd.ZstdInputStream;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher;
import org.hiero.block.tools.blocks.model.hashing.HashingUtils;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.blocks.wrapped.RunningAccountsState;
import org.hiero.block.tools.blocks.wrapped.WrappedBlockValidator;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.SigFileUtils;
import org.hiero.block.tools.records.model.parsed.ParsedRecordFile;
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
 * <p>Validation can be interrupted and resumed: a {@code validateCheckpoint/} directory inside the first
 * input directory persists progress between runs. Pass {@code --no-resume} to ignore any existing
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

    /** Pattern to extract block number from filename. */
    private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^(\\d+)\\.blk(\\.gz|\\.zstd)?$");

    /** Number of zip files skipped because their central directory was corrupt. */
    private long corruptZipCount = 0;

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

    /** A decoded block together with its block number (from the zip entry filename). */
    private record ParsedBlock(Block block, long blockNumber) {}

    /** Record representing a block source (file or zip entry). */
    private record BlockSource(long blockNumber, Path filePath, String zipEntryName) {
        boolean isZipEntry() {
            return zipEntryName != null;
        }
    }

    /** Lightweight state loaded from a checkpoint JSON file. */
    private record CheckpointState(long lastValidatedBlockNumber, long blocksValidated, byte[] previousBlockHash) {}

    /**
     * Saves checkpoint state atomically using {@link HasherStateFiles#saveAtomically}.
     *
     * <p>Only progress state is saved — error counters are not persisted because the command
     * is fail-fast (stops on the first error), so they would always be zero on a successful resume.
     *
     * @param checkpointDir directory to write checkpoint files into
     * @param lastValidatedBlockNumber the last block number that was fully validated
     * @param blocksValidated number of blocks validated so far
     * @param previousBlockHash hash of the last validated block
     * @param freshStreamingHasher streaming hasher state to persist
     * @param accounts running account state to persist (may be null)
     */
    private static void saveCheckpoint(
            Path checkpointDir,
            long lastValidatedBlockNumber,
            long blocksValidated,
            byte[] previousBlockHash,
            StreamingHasher freshStreamingHasher,
            RunningAccountsState accounts) {
        // Save StreamingHasher binary state atomically
        try {
            HasherStateFiles.saveAtomically(
                    checkpointDir.resolve("validateStreamingHasher.bin"), freshStreamingHasher::save);
        } catch (Exception e) {
            System.err.println("Warning: could not save streaming hasher: " + e.getMessage());
        }
        // Save RunningAccountsState binary state atomically
        if (accounts != null) {
            try {
                HasherStateFiles.saveAtomically(checkpointDir.resolve("validateAccountState.bin"), accounts::save);
            } catch (Exception e) {
                System.err.println("Warning: could not save account state: " + e.getMessage());
            }
        }
        // Build and save JSON checkpoint atomically
        JsonObject root = new JsonObject();
        root.addProperty("schemaVersion", 3);
        root.addProperty("lastValidatedBlockNumber", lastValidatedBlockNumber);
        root.addProperty("blocksValidated", blocksValidated);
        root.addProperty("previousBlockHashHex", Bytes.wrap(previousBlockHash).toHex());
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
        // Check if any input is a directory containing addressBookHistory.json
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
        List<BlockSource> sources = findBlockSources(files);
        if (sources.isEmpty()) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No block files found"));
            return;
        }

        // Sort by block number
        sources.sort(Comparator.comparingLong(BlockSource::blockNumber));

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
        // pendingSources = only blocks still needing validation
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

        // Validation tracking — error counters always start at 0 (fail-fast stops on first error)
        final long startNanos = System.nanoTime();
        final AtomicLong blocksValidated = new AtomicLong(checkpoint != null ? checkpoint.blocksValidated() : 0);
        final AtomicLong hashErrors = new AtomicLong(0);
        final AtomicLong signatureErrors = new AtomicLong(0);
        final AtomicLong otherErrors = new AtomicLong(0);
        final AtomicLong stateFileErrors = new AtomicLong(0);
        final AtomicLong structureErrors = new AtomicLong(0);
        final AtomicLong treeRootErrors = new AtomicLong(0);
        final AtomicLong balanceErrors = new AtomicLong(0);
        final AtomicReference<byte[]> previousBlockHash =
                new AtomicReference<>(checkpoint != null ? checkpoint.previousBlockHash() : null);

        // Running accounts state for 50 billion HBAR supply validation
        final RunningAccountsState accounts = new RunningAccountsState();

        // Check for missing companion state files upfront
        if (hasStateFiles) {
            if (!Files.exists(streamingMerkleTreePath)) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ streamingMerkleTree.bin not found alongside blockStreamBlockHashes.bin"));
                stateFileErrors.incrementAndGet();
            }
            if (!Files.exists(completeMerkleTreePath)) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ completeMerkleTree.bin not found alongside blockStreamBlockHashes.bin"));
                stateFileErrors.incrementAndGet();
            }
            if (!Files.exists(jumpstartPath)) {
                System.err.println(Ansi.AUTO.string(
                        "@|red Error:|@ jumpstart.bin not found alongside blockStreamBlockHashes.bin"));
                stateFileErrors.incrementAndGet();
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

        // Open the hash registry for per-block validation (null when no state files present).
        // Closed in the final block below; declared here (not in a try-with-resources) because
        // the try block starts well after the setup code that also needs this reference.
        BlockStreamBlockHashRegistry registry =
                hasStateFiles ? new BlockStreamBlockHashRegistry(hashRegistryPath) : null;
        final StreamingHasher freshStreamingHasher = new StreamingHasher();

        // Restore StreamingHasher state from the checkpoint binary (if resuming)
        if (checkpoint != null) {
            try {
                Files.createDirectories(checkpointDir);
            } catch (IOException ignored) {
            }
            HasherStateFiles.loadWithFallback(
                    checkpointDir.resolve("validateStreamingHasher.bin"), freshStreamingHasher::load);
            if (freshStreamingHasher.leafCount() > 0) {
                System.out.println(Ansi.AUTO.string(
                        "@|yellow Restored streaming hasher:|@ leafCount = " + freshStreamingHasher.leafCount()));
            }
            HasherStateFiles.loadWithFallback(checkpointDir.resolve("validateAccountState.bin"), accounts::load);
        }

        // Rebuild InMemoryTreeHasher by replaying already-validated block hashes from registry
        final InMemoryTreeHasher freshInMemoryHasher = new InMemoryTreeHasher();
        if (checkpoint != null && registry != null && checkpoint.lastValidatedBlockNumber() >= 0) {
            long firstBlock = sources.getFirst().blockNumber();
            long replayTo = checkpoint.lastValidatedBlockNumber();
            System.out.println(Ansi.AUTO.string("@|yellow Replaying |@" + (replayTo - firstBlock + 1)
                    + " block hashes into in-memory hasher (blocks " + firstBlock + ".." + replayTo + ")"));
            for (long bn = firstBlock; bn <= replayTo; bn++) {
                freshInMemoryHasher.addNodeByHash(registry.getBlockHash(bn));
            }
        }

        // Track the last successfully validated block for checkpoint saving in shutdown hook
        final long[] lastValidatedRef = {checkpoint != null ? checkpoint.lastValidatedBlockNumber() : -1L};

        // Flag set in the final block to prevent the shutdown hook from double-saving.
        final boolean[] completed = {false};

        // Shutdown hook: save checkpoint if interrupted mid-run (e.g. Ctrl-C).
        // The hook is removed after a normal exit to avoid a spurious save.
        final Thread shutdownHook = new Thread(
                () -> {
                    if (!completed[0] && lastValidatedRef[0] >= 0 && previousBlockHash.get() != null) {
                        System.err.println("Shutdown: saving validation checkpoint at block " + lastValidatedRef[0]);
                        try {
                            Files.createDirectories(checkpointDir);
                        } catch (IOException ignored) {
                        }
                        saveCheckpoint(
                                checkpointDir,
                                lastValidatedRef[0],
                                blocksValidated.get(),
                                previousBlockHash.get(),
                                freshStreamingHasher,
                                accounts);
                    }
                },
                "validate-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // ── Pipeline ─────────────────────────────────────────────────────────
        // Stage 1 (I/O thread)   : stream each zip with ZipInputStream (sequential HDD reads),
        //                          submit decompression futures to pool
        // Stage 2 (decompPool)   : decompress + parse protobuf in parallel
        // Stage 3 (main thread)  : validate in order from blockQueue

        // Decompression pool uses daemon threads, so it never prevents JVM exit.
        // try-with-resources not used: pool is shut down explicitly after the validation loop.
        ExecutorService decompPool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        // Sentinel: unique object used to signal end-of-stream; detected by reference equality.
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
                                // Regular file: read + decompress in pool
                                final Path path = first.filePath();
                                final String fname = path.getFileName().toString();
                                final boolean isZstd = fname.endsWith(".zstd");
                                final boolean isGz = fname.endsWith(".gz");
                                final long bNum = first.blockNumber();
                                blockQueue.put(decompPool.submit(() -> {
                                    byte[] raw = Files.readAllBytes(path);
                                    return new ParsedBlock(decompressAndParse(raw, isZstd, isGz), bNum);
                                }));
                                i++;
                            } else {
                                // Find the run of consecutive sources from the same zip
                                Path zipPath = first.filePath();
                                int runEnd = i;
                                Set<String> wanted = new HashSet<>();
                                while (runEnd < pendingSources.size()
                                        && pendingSources.get(runEnd).isZipEntry()
                                        && pendingSources.get(runEnd).filePath().equals(zipPath)) {
                                    // ZipFileSystem paths start with "/" but ZipInputStream names do not
                                    String en = pendingSources.get(runEnd).zipEntryName();
                                    if (en.startsWith("/")) en = en.substring(1);
                                    wanted.add(en);
                                    runEnd++;
                                }

                                // Stream this zip sequentially — zero random seeks
                                try (ZipInputStream zis = new ZipInputStream(
                                        new BufferedInputStream(Files.newInputStream(zipPath), ZIP_READ_BUFFER))) {
                                    ZipEntry entry;
                                    while ((entry = zis.getNextEntry()) != null) {
                                        final String entryName = entry.getName(); // no leading slash
                                        if (!wanted.contains(entryName)) {
                                            zis.closeEntry();
                                            continue;
                                        }
                                        final boolean isZstd = entryName.endsWith(".zstd");
                                        final boolean isGz = entryName.endsWith(".gz");
                                        final byte[] raw = zis.readAllBytes();
                                        final String fileName = entryName.substring(entryName.lastIndexOf('/') + 1);
                                        final long bNum = extractBlockNumber(fileName);
                                        blockQueue.put(decompPool.submit(
                                                () -> new ParsedBlock(decompressAndParse(raw, isZstd, isGz), bNum)));
                                    }
                                } catch (IOException e) {
                                    corruptZipCount++;
                                    System.out.println(Ansi.AUTO.string(
                                            "@|yellow Warning:|@ error streaming zip (blocks skipped): "
                                                    + zipPath.getFileName() + " — " + e.getMessage()));
                                    // Push null futures so the validator loop stays in sync
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
                        // Always signal end-of-stream
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

        try {
            long lastCheckpointSaveMs = System.currentTimeMillis();
            long lastProgressMs = System.currentTimeMillis();

            // Validation loop (main thread)
            for (int i = 0; i < pendingSources.size(); i++) {
                // Fail fast: stop on the first real validation error
                long totalValidationErrors = hashErrors.get()
                        + signatureErrors.get()
                        + otherErrors.get()
                        + structureErrors.get()
                        + treeRootErrors.get()
                        + balanceErrors.get();
                if (totalValidationErrors > 0) {
                    PrettyPrint.clearProgress();
                    System.err.println(Ansi.AUTO.string("@|red Stopping validation (fail-fast):|@ error detected"));
                    break;
                }

                long blockNum = pendingSources.get(i).blockNumber();

                try {
                    Future<ParsedBlock> future = blockQueue.take();
                    if (future == endOfStream) break; // reader exited early

                    ParsedBlock parsed = future.get();
                    if (parsed == null) continue; // block skipped (corrupt zip mid-stream)

                    Block block = parsed.block();

                    // 1. Validate block structure (required items + ordering)
                    if (!validateBlockStructure(blockNum, block, structureErrors)) {
                        break;
                    }

                    // Extract block proof and previous block hash from the block footer
                    BlockProof blockProof = null;
                    byte[] previousHashInBlock = null;
                    for (BlockItem item : block.items()) {
                        if (item.hasBlockProof()) {
                            blockProof = item.blockProof();
                        }
                        if (item.hasBlockFooter()) {
                            previousHashInBlock = item.blockFooterOrThrow()
                                    .previousBlockRootHash()
                                    .toByteArray();
                        }
                    }

                    // 2. Compute this block's hash using the proper 16-leaf Merkle tree algorithm
                    byte[] currentBlockHash = BlockStreamBlockHasher.hashBlock(block);

                    // 3. Validate hash chain
                    boolean hashValid =
                            validateHashChain(blockNum, previousHashInBlock, previousBlockHash.get(), hashErrors);

                    // 4. Validate historical block tree root (before adding this block's hash)
                    validateHistoricalBlockTreeRoot(blockNum, block, freshStreamingHasher, treeRootErrors);

                    // 5. Update previous hash and feed hashers
                    previousBlockHash.set(currentBlockHash);
                    freshStreamingHasher.addNodeByHash(currentBlockHash);
                    freshInMemoryHasher.addNodeByHash(currentBlockHash);

                    // 6. Validate per-block hash against the registry (if state files present)
                    if (registry != null) {
                        byte[] storedHash = registry.getBlockHash(blockNum);
                        if (!Arrays.equals(currentBlockHash, storedHash)) {
                            PrettyPrint.clearProgress();
                            System.out.println(Ansi.AUTO.string(
                                    "@|red Block " + blockNum + ":|@ hash mismatch in blockStreamBlockHashes.bin"));
                            stateFileErrors.incrementAndGet();
                        }
                    }

                    // 7. Validate 50 billion HBAR supply
                    try {
                        WrappedBlockValidator.validate50Billion(blockNum, block, accounts);
                    } catch (ValidationException e) {
                        PrettyPrint.clearProgress();
                        System.out.println(
                                Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Balance error: " + e.getMessage()));
                        balanceErrors.incrementAndGet();
                    }

                    // 8. Validate signatures
                    boolean signaturesValid = true;
                    if (blockProof != null) {
                        signaturesValid = validateSignatures(
                                blockNum, block, blockProof, addressBookRegistry, signatureErrors, currentBlockHash);
                    }

                    // Update last-validated reference and save periodic checkpoint
                    lastValidatedRef[0] = blockNum;
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastCheckpointSaveMs >= 60_000L) {
                        try {
                            Files.createDirectories(checkpointDir);
                        } catch (IOException ignored) {
                        }
                        saveCheckpoint(
                                checkpointDir,
                                blockNum,
                                blocksValidated.get() + 1,
                                currentBlockHash,
                                freshStreamingHasher,
                                accounts);
                        lastCheckpointSaveMs = nowMs;
                    }

                    if (verbose) {
                        String status = (hashValid && signaturesValid)
                                ? Ansi.AUTO.string("@|green VALID|@")
                                : Ansi.AUTO.string("@|red INVALID|@");
                        System.out.println("Block " + blockNum + ": " + status + " (hash: "
                                + Bytes.wrap(currentBlockHash).toHex().substring(0, 8) + ")");
                    }

                    blocksValidated.incrementAndGet();

                    // Progress every 5 seconds
                    if (nowMs - lastProgressMs >= 5_000L || i == pendingSources.size() - 1) {
                        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                        long currentPercent = (blocksValidated.get() * 100) / sources.size();
                        long remainingMillis = PrettyPrint.computeRemainingMilliseconds(
                                blocksValidated.get(), sources.size(), elapsedMillis);
                        String progressString = "Validated " + blocksValidated.get() + "/" + sources.size() + " blocks";
                        PrettyPrint.printProgressWithEta(currentPercent, progressString, remainingMillis);
                        lastProgressMs = nowMs;
                    }

                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.err.println(
                            Ansi.AUTO.string("@|red Error processing block " + blockNum + ":|@ " + e.getMessage()));
                    if (verbose) {
                        e.printStackTrace();
                    }
                    otherErrors.incrementAndGet();
                }
            }

            // Stop reader and decompression pool
            readerThread.interrupt();
            decompPool.shutdownNow();

            // --- Post-loop: validate binary state files ---

            // Validate blockStreamBlockHashes.bin highest stored block number
            if (registry != null) {
                long expectedHighest = sources.getLast().blockNumber();
                if (registry.highestBlockNumberStored() != expectedHighest) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ blockStreamBlockHashes.bin highest stored block "
                                    + registry.highestBlockNumberStored() + " != expected " + expectedHighest));
                    stateFileErrors.incrementAndGet();
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
                        stateFileErrors.incrementAndGet();
                    }
                    if (!Arrays.equals(loadedStreaming.computeRootHash(), freshStreamingHasher.computeRootHash())) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red State file error:|@ streamingMerkleTree.bin root hash mismatch"));
                        stateFileErrors.incrementAndGet();
                    }
                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ Failed to load streamingMerkleTree.bin: " + e.getMessage()));
                    stateFileErrors.incrementAndGet();
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
                        stateFileErrors.incrementAndGet();
                    }
                    if (!Arrays.equals(loadedInMemory.computeRootHash(), freshInMemoryHasher.computeRootHash())) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red State file error:|@ completeMerkleTree.bin root hash mismatch"));
                        stateFileErrors.incrementAndGet();
                    }
                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ Failed to load completeMerkleTree.bin: " + e.getMessage()));
                    stateFileErrors.incrementAndGet();
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
                        stateFileErrors.incrementAndGet();
                    }
                    if (registry != null) {
                        byte[] registryHash = registry.getBlockHash(jBlockNum);
                        if (!Arrays.equals(jHash, registryHash)) {
                            PrettyPrint.clearProgress();
                            System.out.println(Ansi.AUTO.string(
                                    "@|red State file error:|@ jumpstart.bin block hash does not match registry"));
                            stateFileErrors.incrementAndGet();
                        }
                    }
                    if (jLeafCount != freshStreamingHasher.leafCount()) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string("@|red State file error:|@ jumpstart.bin leaf count "
                                + jLeafCount + " != expected " + freshStreamingHasher.leafCount()));
                        stateFileErrors.incrementAndGet();
                    }
                    StreamingHasher jumpstartHasher = new StreamingHasher(jHashes);
                    if (!Arrays.equals(jumpstartHasher.computeRootHash(), freshStreamingHasher.computeRootHash())) {
                        PrettyPrint.clearProgress();
                        System.out.println(Ansi.AUTO.string(
                                "@|red State file error:|@ jumpstart.bin streaming tree root mismatch"));
                        stateFileErrors.incrementAndGet();
                    }
                } catch (Exception e) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|red State file error:|@ Failed to read jumpstart.bin: " + e.getMessage()));
                    stateFileErrors.incrementAndGet();
                }
            }

        } finally {
            completed[0] = true;
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM already shutting down; hook already fired or was removed
            }
            if (registry != null) {
                try {
                    registry.close();
                } catch (Exception ignored) {
                    // best-effort close
                }
            }
        }

        // Determine total errors
        long totalErrors = hashErrors.get()
                + signatureErrors.get()
                + otherErrors.get()
                + stateFileErrors.get()
                + structureErrors.get()
                + treeRootErrors.get()
                + balanceErrors.get();
        boolean allBlocksValidated = (blocksValidated.get() == sources.size());

        // On full success: remove checkpoint (validation is complete).
        // On partial/error: save a final checkpoint for next resume.
        if (totalErrors == 0 && allBlocksValidated) {
            try {
                Files.deleteIfExists(checkpointDir.resolve("validateProgress.json"));
                Files.deleteIfExists(checkpointDir.resolve("validateProgress.json.bak"));
                Files.deleteIfExists(checkpointDir.resolve("validateStreamingHasher.bin"));
                Files.deleteIfExists(checkpointDir.resolve("validateStreamingHasher.bin.bak"));
                Files.deleteIfExists(checkpointDir.resolve("validateAccountState.bin"));
                Files.deleteIfExists(checkpointDir.resolve("validateAccountState.bin.bak"));
                Files.deleteIfExists(checkpointDir);
                System.out.println(Ansi.AUTO.string("@|yellow Checkpoint deleted|@ (validation complete)"));
            } catch (IOException e) {
                System.err.println("Warning: could not delete checkpoint: " + e.getMessage());
            }
        } else if (lastValidatedRef[0] >= 0 && previousBlockHash.get() != null) {
            try {
                Files.createDirectories(checkpointDir);
            } catch (IOException ignored) {
            }
            saveCheckpoint(
                    checkpointDir,
                    lastValidatedRef[0],
                    blocksValidated.get(),
                    previousBlockHash.get(),
                    freshStreamingHasher,
                    accounts);
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
        System.out.println(Ansi.AUTO.string("@|yellow Blocks validated:|@ " + blocksValidated.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Hash chain errors:|@ " + hashErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Structure errors:|@ " + structureErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Tree root errors:|@ " + treeRootErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Balance errors:|@ " + balanceErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Signature errors:|@ " + signatureErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Other errors:|@ " + otherErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow State file errors:|@ " + stateFileErrors.get()));
        if (corruptZipCount > 0) {
            System.out.println(Ansi.AUTO.string(
                    "@|yellow Corrupt zips skipped:|@ " + corruptZipCount + " (blocks inside not validated)"));
        }

        if (totalErrors == 0) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        } else {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,red VALIDATION FAILED|@ - " + totalErrors + " errors found"));
        }

        long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000L;
        System.out.println(Ansi.AUTO.string("@|yellow Time elapsed:|@ " + elapsedSeconds + " seconds"));
    }

    /**
     * Decompresses raw bytes and parses them as a {@link Block} protobuf.
     *
     * @param raw compressed or uncompressed block bytes
     * @param isZstd true if the bytes are zstd-compressed
     * @param isGz true if the bytes are gzip-compressed
     * @return the parsed Block
     * @throws Exception if decompression or parsing fails
     */
    private static Block decompressAndParse(byte[] raw, boolean isZstd, boolean isGz) throws Exception {
        final byte[] blockBytes;
        if (isZstd) {
            try (InputStream is = new ZstdInputStream(new ByteArrayInputStream(raw))) {
                blockBytes = is.readAllBytes();
            }
        } else if (isGz) {
            try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(raw))) {
                blockBytes = is.readAllBytes();
            }
        } else {
            blockBytes = raw;
        }
        return Block.PROTOBUF.parse(Bytes.wrap(blockBytes));
    }

    /**
     * Validates block structure by checking required items and ordering.
     *
     * @param blockNum the block number
     * @param block the block to validate
     * @param structureErrors counter for structure errors
     * @return true if valid
     */
    private boolean validateBlockStructure(long blockNum, Block block, AtomicLong structureErrors) {
        try {
            WrappedBlockValidator.validateRequiredItems(blockNum, block);
            WrappedBlockValidator.validateNoExtraItems(blockNum, block);
            return true;
        } catch (ValidationException e) {
            PrettyPrint.clearProgress();
            System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Structure error: " + e.getMessage()));
            structureErrors.incrementAndGet();
            return false;
        }
    }

    /**
     * Validates the historical block tree root stored in the block footer.
     *
     * @param blockNum the block number
     * @param block the block to validate
     * @param streamingHasher the streaming hasher with current tree state
     * @param treeRootErrors counter for tree root errors
     * @return true if valid
     */
    private boolean validateHistoricalBlockTreeRoot(
            long blockNum, Block block, StreamingHasher streamingHasher, AtomicLong treeRootErrors) {
        try {
            WrappedBlockValidator.validateHistoricalBlockTreeRoot(blockNum, block, streamingHasher);
            return true;
        } catch (ValidationException e) {
            PrettyPrint.clearProgress();
            System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Tree root error: " + e.getMessage()));
            treeRootErrors.incrementAndGet();
            return false;
        }
    }

    /**
     * Validates the hash chain for a block.
     *
     * @param blockNum the block number
     * @param previousHashInBlock the previous hash stored in the block footer
     * @param computedPreviousHash the computed hash of the previous block (null if this is the first block)
     * @param hashErrors counter for hash errors
     * @return true if valid
     */
    private boolean validateHashChain(
            long blockNum, byte[] previousHashInBlock, byte[] computedPreviousHash, AtomicLong hashErrors) {

        if (previousHashInBlock == null) {
            PrettyPrint.clearProgress();
            System.out.println(
                    Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Missing previousBlockRootHash in footer"));
            hashErrors.incrementAndGet();
            return false;
        }

        if (computedPreviousHash == null) {
            // This is the first block processed — its previousBlockRootHash must be the empty-tree hash
            // (the value used by ToWrappedBlocksCommand for the genesis block)
            if (!Arrays.equals(previousHashInBlock, HashingUtils.EMPTY_TREE_HASH)) {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string(
                        "@|red Block " + blockNum + ":|@ First block should have empty-tree previous hash"));
                System.out.println("  Expected: "
                        + Bytes.wrap(HashingUtils.EMPTY_TREE_HASH).toHex());
                System.out.println(
                        "  Found:    " + Bytes.wrap(previousHashInBlock).toHex());
                hashErrors.incrementAndGet();
                return false;
            }
        } else {
            // Check that previous hash matches computed hash
            if (!Arrays.equals(previousHashInBlock, computedPreviousHash)) {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Hash chain broken"));
                System.out.println(
                        "  Expected: " + Bytes.wrap(computedPreviousHash).toHex());
                System.out.println(
                        "  Found:    " + Bytes.wrap(previousHashInBlock).toHex());
                hashErrors.incrementAndGet();
                return false;
            }
        }

        return true;
    }

    /**
     * Validates signatures on a block.
     *
     * @param blockNum            the block number
     * @param block               the full block (needed for extracting RecordFileItem and BlockHeader)
     * @param blockProof          the block proof containing signatures
     * @param addressBookRegistry the address book registry for public keys
     * @param signatureErrors     counter for signature errors
     * @param blockHash           the computed block hash (reserved for future TSS signature verification)
     * @return true if valid (1/3 + 1 signatures verified)
     */
    private boolean validateSignatures(
            long blockNum,
            Block block,
            BlockProof blockProof,
            AddressBookRegistry addressBookRegistry,
            AtomicLong signatureErrors,
            byte[] blockHash) {

        try {
            if (blockProof.hasSignedRecordFileProof()) {
                return validateSignedRecordFileProof(blockNum, block, blockProof, addressBookRegistry, signatureErrors);
            } else if (blockProof.hasSignedBlockProof()) {
                // TSS verification not yet implemented — verify non-empty
                Bytes blockSig = blockProof.signedBlockProofOrThrow().blockSignature();
                if (blockSig.length() == 0) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Empty TSS block signature"));
                    signatureErrors.incrementAndGet();
                    return false;
                }
                if (verbose) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string("@|yellow Block " + blockNum
                            + ":|@ TSS signature present but verification not yet implemented"));
                }
                return true;
            } else {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Unknown proof type: "
                        + blockProof.proof().kind()));
                signatureErrors.incrementAndGet();
                return false;
            }
        } catch (Exception e) {
            PrettyPrint.clearProgress();
            System.out.println(
                    Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Signature validation error: " + e.getMessage()));
            if (verbose) {
                e.printStackTrace();
            }
            signatureErrors.incrementAndGet();
            return false;
        }
    }

    /**
     * Validates a SignedRecordFileProof by reconstructing the record file hash and verifying
     * RSA signatures from consensus nodes against the address book.
     *
     * @param blockNum            the block number
     * @param block               the full block
     * @param blockProof          the block proof containing the SignedRecordFileProof
     * @param addressBookRegistry the address book registry for public keys
     * @param signatureErrors     counter for signature errors
     * @return true if the threshold number of signatures verified successfully
     */
    private boolean validateSignedRecordFileProof(
            long blockNum,
            Block block,
            BlockProof blockProof,
            AddressBookRegistry addressBookRegistry,
            AtomicLong signatureErrors) {

        final var signedRecordFileProof = blockProof.signedRecordFileProofOrThrow();
        final var signatures = signedRecordFileProof.recordFileSignatures();

        if (signatures.isEmpty()) {
            PrettyPrint.clearProgress();
            System.out.println(
                    Ansi.AUTO.string("@|red Block " + blockNum + ":|@ No signatures in SignedRecordFileProof"));
            signatureErrors.incrementAndGet();
            return false;
        }

        // Extract RecordFileItem and BlockHeader from the block
        RecordFileItem recordFileItem = null;
        BlockHeader blockHeader = null;
        for (BlockItem item : block.items()) {
            if (item.hasRecordFile()) recordFileItem = item.recordFileOrThrow();
            if (item.hasBlockHeader()) blockHeader = item.blockHeaderOrThrow();
        }
        if (recordFileItem == null || blockHeader == null) {
            PrettyPrint.clearProgress();
            System.out.println(Ansi.AUTO.string("@|red Block " + blockNum
                    + ":|@ Missing RecordFileItem or BlockHeader for signature verification"));
            signatureErrors.incrementAndGet();
            return false;
        }

        // Reconstruct the signed hash via ParsedRecordFile
        final Timestamp creationTime = recordFileItem.creationTime();
        final Instant blockTime = Instant.ofEpochSecond(creationTime.seconds(), creationTime.nanos());
        final byte[] startHash = recordFileItem
                .recordFileContentsOrThrow()
                .startObjectRunningHash()
                .hash()
                .toByteArray();
        final ParsedRecordFile parsedRecordFile = ParsedRecordFile.parse(
                blockTime,
                signedRecordFileProof.version(),
                blockHeader.hapiProtoVersion(),
                startHash,
                recordFileItem.recordFileContentsOrThrow());
        final byte[] signedHash = parsedRecordFile.signedHash();

        // Get the address book for this block's timestamp
        final NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(blockTime);
        final int totalNodes = addressBook.nodeAddress().size();
        final int threshold = (totalNodes / 3) + 1;

        // Verify each signature
        int validCount = 0;
        int failedCount = 0;
        for (var sig : signatures) {
            final long accountNum = AddressBookRegistry.accountIdForNode(sig.nodeId());
            try {
                final String pubKeyHex = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
                if (SigFileUtils.verifyRsaSha384(
                        pubKeyHex, signedHash, sig.signaturesBytes().toByteArray())) {
                    validCount++;
                } else {
                    failedCount++;
                }
            } catch (Exception e) {
                // Unknown node — skip but count as failed
                failedCount++;
            }
        }

        if (validCount < threshold) {
            PrettyPrint.clearProgress();
            System.out.println(Ansi.AUTO.string("@|red Block " + blockNum
                    + ":|@ Insufficient valid signatures: " + validCount + "/" + signatures.size()
                    + " verified, need " + threshold + "/" + totalNodes));
            signatureErrors.incrementAndGet();
            return false;
        }

        return true;
    }

    /**
     * Finds all block sources from the given files/directories.
     *
     * @param files array of files or directories
     * @return list of block sources
     */
    private List<BlockSource> findBlockSources(File[] files) {
        List<BlockSource> sources = new ArrayList<>();

        for (File file : files) {
            if (file.isDirectory()) {
                // Recursively find blocks in the directory
                findBlocksInDirectory(file.toPath(), sources);
            } else if (file.getName().endsWith(".zip")) {
                // Find blocks in a zip file
                findBlocksInZip(file.toPath(), sources);
            } else {
                // Single block file
                long blockNum = extractBlockNumber(file.getName());
                if (blockNum >= 0) {
                    sources.add(new BlockSource(blockNum, file.toPath(), null));
                }
            }
        }

        return sources;
    }

    /**
     * Recursively finds block files in a directory.
     *
     * @param dir the directory to search
     * @param sources list to add sources to
     */
    private void findBlocksInDirectory(Path dir, List<BlockSource> sources) {
        try (Stream<Path> filesStream = Files.walk(dir)) {
            filesStream.filter(Files::isRegularFile).forEach(path -> {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".zip")) {
                    findBlocksInZip(path, sources);
                } else {
                    long blockNum = extractBlockNumber(fileName);
                    if (blockNum >= 0) {
                        sources.add(new BlockSource(blockNum, path, null));
                    }
                }
            });
        } catch (IOException e) {
            System.err.println("Error scanning directory " + dir + ": " + e.getMessage());
        }
    }

    /**
     * Finds block files inside a zip archive.
     *
     * @param zipPath path to the zip file
     * @param sources list to add sources to
     */
    private void findBlocksInZip(Path zipPath, List<BlockSource> sources) {
        try (FileSystem zipFs = FileSystems.newFileSystem(zipPath)) {
            for (Path root : zipFs.getRootDirectories()) {
                try (Stream<Path> filesStream = Files.walk(root)) {
                    filesStream.filter(Files::isRegularFile).forEach(path -> {
                        String fileName = path.getFileName().toString();
                        long blockNum = extractBlockNumber(fileName);
                        if (blockNum >= 0) {
                            sources.add(new BlockSource(blockNum, zipPath, path.toString()));
                        }
                    });
                }
            }
        } catch (IOException e) {
            corruptZipCount++;
            System.out.println(Ansi.AUTO.string("@|yellow Warning:|@ skipping corrupt zip (validation continues): "
                    + zipPath.getFileName() + " — " + e.getMessage()));
        }
    }

    /**
     * Extracts block number from a filename.
     *
     * @param fileName the filename
     * @return the block number, or -1 if not a valid block file
     */
    private long extractBlockNumber(String fileName) {
        Matcher matcher = BLOCK_FILE_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1;
    }
}
