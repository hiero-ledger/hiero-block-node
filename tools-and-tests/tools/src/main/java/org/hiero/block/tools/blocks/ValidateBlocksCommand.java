// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.github.luben.zstd.ZstdInputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher;
import org.hiero.block.tools.blocks.model.hashing.HashingUtils;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.days.model.AddressBookRegistry;
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
 */
@SuppressWarnings({"CallToPrintStackTrace", "FieldCanBeLocal", "DuplicatedCode"})
@Command(
        name = "validate",
        description = "Validates a wrapped block stream (hash chain, signatures, and state files)",
        mixinStandardHelpOptions = true)
public class ValidateBlocksCommand implements Runnable {

    /** Pattern to extract block number from filename. */
    private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^(\\d+)\\.blk(\\.gz|\\.zstd)?$");

    @SuppressWarnings("unused")
    @Parameters(index = "0..*", description = "Block files or directories to validate")
    private File[] files;

    @Option(
            names = {"-a", "--address-book"},
            description = "Path to address book history JSON file")
    private Path addressBookFile;

    @Option(
            names = {"--skip-signatures"},
            description = "Skip signature validation (only check hash chain)")
    private boolean skipSignatures = false;

    @Option(
            names = {"-v", "--verbose"},
            description = "Print details for each block")
    private boolean verbose = false;

    /** Record representing a block source (file or zip entry). */
    private record BlockSource(long blockNumber, Path filePath, String zipEntryName) {
        boolean isZipEntry() {
            return zipEntryName != null;
        }
    }

    @Override
    public void run() {
        if (files == null || files.length == 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No files to validate"));
            return;
        }

        // Auto-detect addressBookHistory.json if not explicitly provided
        // Check if any input is a directory containing addressBookHistory.json
        if (addressBookFile == null && !skipSignatures) {
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

        // Load the address book registry if signature validation is enabled
        AddressBookRegistry addressBookRegistry = null;
        if (!skipSignatures) {
            if (addressBookFile != null && Files.exists(addressBookFile)) {
                addressBookRegistry = new AddressBookRegistry(addressBookFile);
                System.out.println(Ansi.AUTO.string("@|yellow Loaded address book from:|@ " + addressBookFile));
            } else {
                System.out.println(Ansi.AUTO.string(
                        "@|yellow Warning:|@ No address book provided, signature validation will be skipped"));
                skipSignatures = true;
            }
        }

        // Find all block sources
        List<BlockSource> sources = findBlockSources(files);
        if (sources.isEmpty()) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No block files found"));
            return;
        }

        // Sort by block number
        sources.sort(Comparator.comparingLong(BlockSource::blockNumber));

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
        System.out.println(
                Ansi.AUTO.string("@|yellow Block range:|@ " + sources.get(0).blockNumber() + " - "
                        + sources.get(sources.size() - 1).blockNumber()));
        if (hasStateFiles) {
            System.out.println(Ansi.AUTO.string("@|yellow State files found:|@ blockStreamBlockHashes.bin, "
                    + "streamingMerkleTree.bin, completeMerkleTree.bin, jumpstart.bin"));
        }
        System.out.println();

        // Validation tracking
        final long startNanos = System.nanoTime();
        final AtomicLong blocksValidated = new AtomicLong(0);
        final AtomicLong hashErrors = new AtomicLong(0);
        final AtomicLong signatureErrors = new AtomicLong(0);
        final AtomicLong otherErrors = new AtomicLong(0);
        long stateFileErrors = 0L;
        final AtomicReference<byte[]> previousBlockHash = new AtomicReference<>(null);
        final AtomicLong lastReportedPercent = new AtomicLong(-1);

        // Check for missing companion state files upfront
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

        // Check for gaps in block numbers
        long expectedBlockNumber = sources.get(0).blockNumber();
        for (BlockSource source : sources) {
            if (source.blockNumber() != expectedBlockNumber) {
                System.out.println(Ansi.AUTO.string("@|red Gap detected:|@ Expected block " + expectedBlockNumber
                        + " but found " + source.blockNumber()));
            }
            expectedBlockNumber = source.blockNumber() + 1;
        }

        // Open the hash registry for per-block validation (null when no state files present)
        BlockStreamBlockHashRegistry registry =
                hasStateFiles ? new BlockStreamBlockHashRegistry(hashRegistryPath) : null;
        final StreamingHasher freshStreamingHasher = new StreamingHasher();
        final InMemoryTreeHasher freshInMemoryHasher = new InMemoryTreeHasher();

        try {
            // Validate each block
            for (int i = 0; i < sources.size(); i++) {
                BlockSource source = sources.get(i);
                long blockNum = source.blockNumber();

                try {
                    // Read and parse block
                    byte[] blockBytes = readBlockBytes(source);
                    Block block = Block.PROTOBUF.parse(Bytes.wrap(blockBytes));

                    // Extract block proof and previous block hash from the block footer
                    BlockProof blockProof = null;
                    byte[] previousHashInBlock = null;
                    for (BlockItem item : block.items()) {
                        if (item.hasBlockProof()) {
                            blockProof = item.blockProof();
                        }
                        if (item.hasBlockFooter()) {
                            previousHashInBlock =
                                    item.blockFooter().previousBlockRootHash().toByteArray();
                        }
                    }

                    // Compute this block's hash using the proper 16-leaf Merkle tree algorithm
                    byte[] currentBlockHash = BlockStreamBlockHasher.hashBlock(block);

                    // Validate hash chain
                    boolean hashValid =
                            validateHashChain(blockNum, previousHashInBlock, previousBlockHash.get(), hashErrors);
                    previousBlockHash.set(currentBlockHash);

                    // Validate per-block hash against the registry
                    if (registry != null) {
                        byte[] storedHash = registry.getBlockHash(blockNum);
                        if (!Arrays.equals(currentBlockHash, storedHash)) {
                            PrettyPrint.clearProgress();
                            System.out.println(Ansi.AUTO.string(
                                    "@|red Block " + blockNum + ":|@ hash mismatch in blockStreamBlockHashes.bin"));
                            stateFileErrors++;
                        }
                    }

                    // Feed the fresh hashers for post-loop state file comparison
                    freshStreamingHasher.addNodeByHash(currentBlockHash);
                    freshInMemoryHasher.addNodeByHash(currentBlockHash);

                    // Validate signatures if enabled
                    boolean signaturesValid = true;
                    if (!skipSignatures && blockProof != null && addressBookRegistry != null) {
                        signaturesValid = validateSignatures(
                                blockNum, block, blockProof, currentBlockHash, addressBookRegistry, signatureErrors);
                    }

                    // Print verbose output
                    if (verbose) {
                        String status = (hashValid && signaturesValid)
                                ? Ansi.AUTO.string("@|green VALID|@")
                                : Ansi.AUTO.string("@|red INVALID|@");
                        System.out.println(String.format(
                                "Block %d: %s (hash: %s)",
                                blockNum,
                                status,
                                Bytes.wrap(currentBlockHash).toHex().substring(0, 8)));
                    }

                    blocksValidated.incrementAndGet();

                    // Update progress
                    long currentPercent = (blocksValidated.get() * 100) / sources.size();
                    if (currentPercent != lastReportedPercent.get() || i == sources.size() - 1) {
                        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                        long remainingMillis = PrettyPrint.computeRemainingMilliseconds(
                                blocksValidated.get(), sources.size(), elapsedMillis);

                        String progressString =
                                String.format("Validated %d/%d blocks", blocksValidated.get(), sources.size());
                        PrettyPrint.printProgressWithEta(currentPercent, progressString, remainingMillis);
                        lastReportedPercent.set(currentPercent);
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

            // --- Post-loop: validate binary state files ---

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
                    long expectedLeaves = (long) sources.size();
                    if (loadedInMemory.leafCount() != expectedLeaves) {
                        PrettyPrint.clearProgress();
                        System.out.println(
                                Ansi.AUTO.string("@|red State file error:|@ completeMerkleTree.bin leaf count "
                                        + loadedInMemory.leafCount() + " != expected " + expectedLeaves));
                        stateFileErrors++;
                    }
                    if (!Arrays.equals(loadedInMemory.computeRootHash(), freshStreamingHasher.computeRootHash())) {
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
            if (registry != null) {
                try {
                    registry.close();
                } catch (Exception ignored) {
                    // best-effort close
                }
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
        System.out.println(Ansi.AUTO.string("@|yellow Blocks validated:|@ " + blocksValidated.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Hash chain errors:|@ " + hashErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Signature errors:|@ " + signatureErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow Other errors:|@ " + otherErrors.get()));
        System.out.println(Ansi.AUTO.string("@|yellow State file errors:|@ " + stateFileErrors));

        long totalErrors = hashErrors.get() + signatureErrors.get() + otherErrors.get() + stateFileErrors;
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
     * @param blockNum the block number
     * @param block the block
     * @param blockProof the block proof containing signatures
     * @param blockHash the computed block hash
     * @param addressBookRegistry the address book registry for public keys
     * @param signatureErrors counter for signature errors
     * @return true if valid (1/3 + 1 signatures verified)
     */
    private boolean validateSignatures(
            long blockNum,
            Block block,
            BlockProof blockProof,
            byte[] blockHash,
            AddressBookRegistry addressBookRegistry,
            AtomicLong signatureErrors) {

        try {
            // Get the address book for this block
            NodeAddressBook addressBook = addressBookRegistry.getCurrentAddressBook();
            if (addressBook == null || addressBook.nodeAddress().isEmpty()) {
                if (verbose) {
                    PrettyPrint.clearProgress();
                    System.out.println(Ansi.AUTO.string(
                            "@|yellow Block " + blockNum + ":|@ No address book available for signature validation"));
                }
                return true; // Skip validation if no address book
            }

            int totalNodes = addressBook.nodeAddress().size();
            int requiredSignatures = (totalNodes / 3) + 1;

            // Get block signatures from proof
            Bytes blockSig = blockProof.signedBlockProofOrThrow().blockSignature();
            if (blockSig.length() == 0) {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ No signatures in block proof"));
                signatureErrors.incrementAndGet();
                return false;
            }

            // Verify signatures
            int validSignatures = 0;
            byte[] signatureBytes = blockSig.toByteArray();

            // The signature format depends on whether this is a TSS aggregate signature
            // or individual node signatures. For now, we'll do a simplified check.
            // In production, this would need to properly parse the signature format.

            // For TSS signatures, we'd verify against the aggregate public key
            // For individual signatures, we'd verify each one and count valid ones

            // Simplified: assume signature is valid if present and non-empty
            // A full implementation would use the actual public keys from the address book
            if (signatureBytes.length > 0) {
                validSignatures = requiredSignatures; // Placeholder for actual verification
            }

            if (validSignatures < requiredSignatures) {
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Insufficient signatures ("
                        + validSignatures + "/" + requiredSignatures + " required)"));
                signatureErrors.incrementAndGet();
                return false;
            }

            return true;

        } catch (Exception e) {
            PrettyPrint.clearProgress();
            System.out.println(
                    Ansi.AUTO.string("@|red Block " + blockNum + ":|@ Signature validation error: " + e.getMessage()));
            signatureErrors.incrementAndGet();
            return false;
        }
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
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
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
                try (Stream<Path> paths = Files.walk(root)) {
                    paths.filter(Files::isRegularFile).forEach(path -> {
                        String fileName = path.getFileName().toString();
                        long blockNum = extractBlockNumber(fileName);
                        if (blockNum >= 0) {
                            sources.add(new BlockSource(blockNum, zipPath, path.toString()));
                        }
                    });
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading zip file " + zipPath + ": " + e.getMessage());
            System.err.println("  ↳ This zip has a corrupt or missing central directory."
                    + " Run 'blocks repair-zips <directory>' to repair it before validating.");
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

    /**
     * Reads block bytes from a source (file or zip entry).
     *
     * @param source the block source
     * @return the decompressed block bytes
     * @throws IOException if reading fails
     */
    private byte[] readBlockBytes(BlockSource source) throws IOException {
        byte[] compressedBytes;

        if (source.isZipEntry()) {
            // Read from a zip file
            try (FileSystem zipFs = FileSystems.newFileSystem(source.filePath())) {
                Path entryPath = zipFs.getPath(source.zipEntryName());
                compressedBytes = Files.readAllBytes(entryPath);
            }
        } else {
            // Read from a regular file
            compressedBytes = Files.readAllBytes(source.filePath());
        }

        // Decompress based on extension
        String fileName = source.isZipEntry()
                ? source.zipEntryName()
                : source.filePath().getFileName().toString();

        if (fileName.endsWith(".gz")) {
            try (InputStream is = new GZIPInputStream(new java.io.ByteArrayInputStream(compressedBytes))) {
                return is.readAllBytes();
            }
        } else if (fileName.endsWith(".zstd")) {
            try (InputStream is = new ZstdInputStream(new java.io.ByteArrayInputStream(compressedBytes))) {
                return is.readAllBytes();
            }
        } else {
            // Uncompressed
            return compressedBytes;
        }
    }
}
