// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.AmendmentProvider.createAmendmentProvider;
import static org.hiero.block.tools.blocks.HasherStateFiles.saveStateCheckpoint;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_COMPRESSION;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_POWERS_OF_TEN_PER_ZIP;
import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamItem;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockZipAppender;
import org.hiero.block.tools.blocks.model.PreVerifiedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;

/**
 * Wraps completed day archives into block stream files. Extracted from
 * {@link ToWrappedBlocksCommand} so the same 4-stage pipeline can be used
 * both from the standalone {@code wrap} CLI and inline from {@code download-live2}.
 *
 * <p>State files stored in the output directory:
 * <ul>
 *   <li>{@code wrap-commit.bin} — durable watermark (highest block whose zip write completed)</li>
 *   <li>{@code blockStreamBlockHashes.bin} — block hash registry</li>
 *   <li>{@code streamingMerkleTree.bin} — hasher checkpoint</li>
 *   <li>{@code addressBookHistory.json} — address book state</li>
 * </ul>
 */
public class DayBlockWrapper implements AutoCloseable {

    private static final String WATERMARK_FILE_NAME = "wrap-commit.bin";
    private static final int WATERMARK_BATCH_SIZE = 256;

    private final Path outputBlocksDir;
    private final AddressBookRegistry addressBookRegistry;
    private final AmendmentProvider amendmentProvider;
    private final BlockTimeReader blockTimeReader;
    private final int parseThreads;

    // State
    private final BlockStreamBlockHashRegistry blockRegistry;
    private final StreamingHasher streamingHasher;
    private final AtomicLong blockCounter;
    private final AtomicLong durableWatermark;
    private final AtomicLong blocksSinceWatermarkFlush;
    private final Path watermarkFile;
    private final Path streamingMerkleTreeFile;
    private final Path addressBookFile;

    // Executor pools
    private final ExecutorService parseAndVerifyPool;
    private final ExecutorService serializePool;
    private final ExecutorService zipWritePool;

    // Long-lived zip state
    private final AtomicReference<BlockZipAppender> currentZipRef = new AtomicReference<>(null);
    private final AtomicReference<Path> currentZipPathRef = new AtomicReference<>(null);

    // Guards chain state updates so close() does not read partially updated state.
    private final Object chainStateLock = new Object();

    // Jumpstart state
    private final Path jumpstartFile;
    private final AtomicLong jumpstartBlockNumber = new AtomicLong(-1);
    private final AtomicReference<byte[]> jumpstartBlockHash = new AtomicReference<>(null);

    private volatile boolean shutdownRequested = false;

    /**
     * Creates a new DayBlockWrapper.
     *
     * @param outputBlocksDir directory for wrapped block output
     * @param addressBookRegistry the address book registry
     * @param blockTimeReader the block time reader
     * @param parseThreads thread count for parse+verify stage
     * @throws Exception if initialization fails
     */
    public DayBlockWrapper(
            Path outputBlocksDir,
            AddressBookRegistry addressBookRegistry,
            BlockTimeReader blockTimeReader,
            int parseThreads)
            throws Exception {
        this.outputBlocksDir = outputBlocksDir;
        this.addressBookRegistry = addressBookRegistry;
        this.blockTimeReader = blockTimeReader;
        this.parseThreads = Math.max(1, parseThreads);

        Files.createDirectories(outputBlocksDir);

        // Copy address book from default location if not present in output dir
        this.addressBookFile = outputBlocksDir.resolve("addressBookHistory.json");

        // Create amendment provider based on network selection
        this.amendmentProvider = createAmendmentProvider(NetworkConfig.current().networkName());

        // Thread pools — lower counts since wrapping runs alongside downloading
        this.parseAndVerifyPool = Executors.newFixedThreadPool(this.parseThreads);
        this.serializePool = Executors.newFixedThreadPool(Math.max(1, this.parseThreads));
        this.zipWritePool = Executors.newSingleThreadExecutor();

        // State files
        this.watermarkFile = outputBlocksDir.resolve(WATERMARK_FILE_NAME);
        this.streamingMerkleTreeFile = outputBlocksDir.resolve("streamingMerkleTree.bin");
        this.jumpstartFile = outputBlocksDir.resolve("jumpstart.bin");

        // Load watermark
        this.durableWatermark = new AtomicLong(loadWatermark(watermarkFile));
        this.blocksSinceWatermarkFlush = new AtomicLong(0);

        // Load block hash registry and reconcile with watermark
        this.blockRegistry = new BlockStreamBlockHashRegistry(outputBlocksDir.resolve("blockStreamBlockHashes.bin"));
        reconcileRegistryWithWatermark();

        // Build streaming hasher from registry
        this.streamingHasher = new StreamingHasher();
        long effectiveHighest = blockRegistry.highestBlockNumberStored();
        if (effectiveHighest >= 0) {
            System.out.println("[WRAP] Replaying " + (effectiveHighest + 1) + " block hashes into hasher (blocks 0.."
                    + effectiveHighest + ")");
            for (long bn = 0; bn <= effectiveHighest; bn++) {
                byte[] hash = blockRegistry.getBlockHash(bn);
                streamingHasher.addNodeByHash(hash);
            }
            System.out.println("[WRAP] Hasher replay complete. leafCount=" + streamingHasher.leafCount());
        }

        // Initialize block counter
        long startBlock = effectiveHighest == -1 ? 0 : effectiveHighest + 1;
        this.blockCounter = new AtomicLong(startBlock);

        System.out.println("[WRAP] DayBlockWrapper initialized. Resume from block " + startBlock);
    }

    /**
     * Returns the highest block number that has been fully wrapped and committed.
     *
     * @return the highest wrapped block number, or -1 if no blocks have been wrapped
     */
    public long highestWrappedBlock() {
        return blockRegistry.highestBlockNumberStored();
    }

    /**
     * Wraps a single completed day archive into block stream files.
     *
     * @param dayArchivePath path to the .tar.zstd day archive
     * @param dayBlockInfo block info for this day (first/last block numbers)
     * @return true on success
     * @throws Exception if wrapping fails (fail-hard)
     */
    public boolean wrapDay(Path dayArchivePath, DayBlockInfo dayBlockInfo) throws Exception {
        long effectiveHighest = blockRegistry.highestBlockNumberStored();
        final long preWrapHighestBlock = effectiveHighest;
        final byte[] preWrapLastBlockHash =
                preWrapHighestBlock >= 0 ? blockRegistry.getBlockHash(preWrapHighestBlock) : null;

        // Skip if this day's blocks are already fully wrapped
        if (effectiveHighest >= dayBlockInfo.lastBlockNumber) {
            System.out.println("[WRAP] Skipping " + dayArchivePath.getFileName() + " — already wrapped through block "
                    + effectiveHighest);
            return true;
        }

        // Determine the time filter for resuming mid-day
        final Instant highestStoredBlockTime = effectiveHighest == -1
                ? FIRST_BLOCK_TIME_INSTANT.minusNanos(1)
                : blockTimeReader.getBlockInstant(effectiveHighest);

        System.out.println("[WRAP] Processing " + dayArchivePath.getFileName() + " (blocks "
                + dayBlockInfo.firstBlockNumber + "-" + dayBlockInfo.lastBlockNumber + ")");

        // Read and process the day archive through the 4-stage pipeline
        CompletableFuture<Void> lastWriteFuture = CompletableFuture.completedFuture(null);

        try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(dayArchivePath)) {
            final Deque<CompletableFuture<PreVerifiedBlock>> parseWindow = new ArrayDeque<>();
            final Iterator<UnparsedRecordBlock> it = stream.filter(
                            recordBlock -> recordBlock.recordFileTime().isAfter(highestStoredBlockTime))
                    .iterator();

            while ((it.hasNext() || !parseWindow.isEmpty()) && !shutdownRequested) {
                // ---- Stage 1: fill the sliding parse+verify window ----
                while (parseWindow.size() < parseThreads && it.hasNext()) {
                    final UnparsedRecordBlock unparsed = it.next();
                    parseWindow.add(CompletableFuture.supplyAsync(
                            () -> {
                                final ParsedRecordBlock parsed = unparsed.parse();
                                final NodeAddressBook ab =
                                        addressBookRegistry.getAddressBookForBlock(parsed.blockTime());
                                final byte[] signedHash = parsed.recordFile().signedHash();
                                final List<RecordFileSignature> sigs = parsed.signatureFiles().stream()
                                        .parallel()
                                        .filter(psf -> psf.isValid(signedHash, ab))
                                        .map(psf -> psf.toRecordFileSignature(ab))
                                        .toList();
                                if (sigs.isEmpty()) {
                                    throw new RuntimeException("Zero verified signatures for block at "
                                            + parsed.blockTime()
                                            + " (sig files in archive: "
                                            + parsed.signatureFiles().size()
                                            + ", address book nodes: "
                                            + ab.nodeAddress().size() + ")");
                                }
                                return new PreVerifiedBlock(parsed, ab, sigs);
                            },
                            parseAndVerifyPool));
                }
                if (parseWindow.isEmpty()) {
                    break;
                }

                // ---- Stage 2: convert + chain-state update (caller thread, sequential) ----
                final PreVerifiedBlock preVerified = parseWindow.poll().join();
                final long blockNum = blockCounter.getAndIncrement();

                // Validate block number matches record file
                final long blockNumberFromRecordFile = preVerified
                        .recordBlock()
                        .recordFile()
                        .recordStreamFile()
                        .blockNumber();
                if (blockNumberFromRecordFile > 0 && blockNum != blockNumberFromRecordFile) {
                    throw new RuntimeException("Block number mismatch at "
                            + preVerified.recordBlock().blockTime()
                            + ": computed blockNum " + blockNum
                            + " != record file block number " + blockNumberFromRecordFile);
                }

                // Address book auto-update
                PreVerifiedBlock effectiveBlock = preVerified;
                try {
                    effectiveBlock = updateAddressBookAndReverify(preVerified, blockNum);
                } catch (Exception e) {
                    System.err.printf("[WRAP] Warning: address book auto-update failed at block %d: %s%n", blockNum, e);
                }

                // Convert record file block to wrapped block
                final Block wrapped = RecordBlockConverter.toBlock(
                        effectiveBlock,
                        blockNum,
                        blockRegistry.mostRecentBlockHash(),
                        streamingHasher.computeRootHash(),
                        amendmentProvider);

                // Update chain state under lock
                final byte[] blockStreamBlockHash = hashBlock(wrapped);
                synchronized (chainStateLock) {
                    streamingHasher.addNodeByHash(blockStreamBlockHash);
                    blockRegistry.addBlock(blockNum, blockStreamBlockHash);
                    jumpstartBlockNumber.set(blockNum);
                    jumpstartBlockHash.set(blockStreamBlockHash);
                }

                // Pre-compute block path
                final BlockPath blockPath =
                        BlockWriter.computeBlockPath(outputBlocksDir, blockNum, BlockArchiveType.UNCOMPRESSED_ZIP);
                Files.createDirectories(blockPath.dirPath());

                // ---- Stage 3: serialize + compress in parallel ----
                final CompletableFuture<byte[]> serFuture = CompletableFuture.supplyAsync(
                        () -> BlockWriter.serializeBlockToBytes(wrapped, DEFAULT_COMPRESSION), serializePool);

                // ---- Stage 4: zip write on single thread ----
                final CompletableFuture<Void> prevWrite = lastWriteFuture;
                final long capturedBlockNum = blockNum;
                lastWriteFuture = CompletableFuture.allOf(prevWrite, serFuture)
                        .thenRunAsync(
                                () -> {
                                    try {
                                        final byte[] bytes = serFuture.join();
                                        // Switch zip file when block number crosses into new range
                                        if (!blockPath.zipFilePath().equals(currentZipPathRef.get())) {
                                            final BlockZipAppender old = currentZipRef.get();
                                            if (old != null) {
                                                old.close();
                                                saveWatermark(watermarkFile, durableWatermark.get());
                                                blocksSinceWatermarkFlush.set(0);
                                            }
                                            currentZipRef.set(null);
                                            currentZipPathRef.set(null);
                                            BlockZipAppender newAppender;
                                            try {
                                                newAppender = BlockWriter.openZipForAppend(blockPath.zipFilePath());
                                            } catch (IOException ex) {
                                                System.err.println(
                                                        "[WRAP] Warning: deleting corrupt zip and recreating: "
                                                                + blockPath.zipFilePath());
                                                Files.deleteIfExists(blockPath.zipFilePath());
                                                newAppender = BlockWriter.openZipForAppend(blockPath.zipFilePath());
                                            }
                                            currentZipRef.set(newAppender);
                                            currentZipPathRef.set(blockPath.zipFilePath());
                                        }
                                        BlockWriter.writeBlockEntry(currentZipRef.get(), blockPath, bytes);
                                        durableWatermark.set(capturedBlockNum);
                                        if (blocksSinceWatermarkFlush.incrementAndGet() >= WATERMARK_BATCH_SIZE) {
                                            saveWatermark(watermarkFile, capturedBlockNum);
                                            blocksSinceWatermarkFlush.set(0);
                                        }
                                    } catch (IOException e) {
                                        final BlockZipAppender leakedZip = currentZipRef.getAndSet(null);
                                        currentZipPathRef.set(null);
                                        if (leakedZip != null) {
                                            try {
                                                leakedZip.close();
                                            } catch (IOException suppressed) {
                                                e.addSuppressed(suppressed);
                                            }
                                        }
                                        throw new UncheckedIOException(e);
                                    }
                                },
                                zipWritePool);
            }
        }

        // Close the last open zip file for this day and flush watermark
        lastWriteFuture
                .thenRunAsync(
                        () -> {
                            try {
                                final BlockZipAppender last = currentZipRef.get();
                                if (last != null) {
                                    last.close();
                                    currentZipRef.set(null);
                                    currentZipPathRef.set(null);
                                }
                                saveWatermark(watermarkFile, durableWatermark.get());
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        },
                        zipWritePool)
                .join();

        // Save state checkpoint
        saveStateCheckpoint(streamingMerkleTreeFile, streamingHasher);
        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);

        // --- Post-wrap boundary validation ---
        final long postWrapHighestBlock = durableWatermark.get();
        if (postWrapHighestBlock > preWrapHighestBlock) {
            final long firstNewBlock = preWrapHighestBlock + 1;
            try {
                BlockReader.closeCachedZipFs(); // ensure fresh read from finalized zip
                validateBoundary(firstNewBlock, preWrapLastBlockHash);
                System.out.println("[WRAP] Post-wrap boundary validation PASSED for block " + firstNewBlock);
            } catch (Exception e) {
                System.err.println("[WRAP] " + e.getMessage());
                rollbackDay(preWrapHighestBlock, postWrapHighestBlock);
                throw new IllegalStateException(
                        "Post-wrap validation failed for " + dayArchivePath.getFileName() + ". Rolled back to block "
                                + preWrapHighestBlock + ". Investigate and restart.",
                        e);
            }
        }

        // Save jumpstart data (only after validation passes)
        if (jumpstartBlockHash.get() != null) {
            ToWrappedBlocksCommand.saveJumpstartData(
                    jumpstartFile, jumpstartBlockNumber.get(), jumpstartBlockHash.get(), streamingHasher);
        }

        System.out.println("[WRAP] Successfully wrapped " + dayArchivePath.getFileName() + " through block "
                + durableWatermark.get());
        return true;
    }

    /**
     * Saves state checkpoint for crash recovery.
     */
    public void saveCheckpoint() {
        synchronized (chainStateLock) {
            saveWatermark(watermarkFile, durableWatermark.get());
            saveStateCheckpoint(streamingMerkleTreeFile, streamingHasher);
            try {
                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
            } catch (Exception e) {
                System.err.println("[WRAP] Warning: could not save address book: " + e.getMessage());
            }
        }
    }

    /**
     * Shuts down executor pools and saves final state.
     */
    @Override
    public void close() {
        shutdownRequested = true;

        // Drain zip write pool
        zipWritePool.shutdown();
        try {
            if (!zipWritePool.awaitTermination(30, TimeUnit.SECONDS)) {
                zipWritePool.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            zipWritePool.shutdownNow();
        }

        // Close any open zip appender
        final BlockZipAppender openZip = currentZipRef.getAndSet(null);
        currentZipPathRef.set(null);
        if (openZip != null) {
            try {
                openZip.close();
            } catch (IOException e) {
                System.err.println("[WRAP] Warning: could not close zip appender: " + e.getMessage());
            }
        }

        // Save state
        saveCheckpoint();

        // Shut down other pools
        parseAndVerifyPool.shutdownNow();
        serializePool.shutdownNow();

        // Close registry
        try {
            blockRegistry.close();
        } catch (Exception e) {
            System.err.println("[WRAP] Warning: could not close block registry: " + e.getMessage());
        }
    }

    // ---- Private helpers ----

    /**
     * Reads the first newly wrapped block back from disk and verifies that its
     * {@code previousBlockRootHash} (in the {@link com.hedera.hapi.block.stream.output.BlockFooter})
     * matches the expected hash of the last pre-existing block.
     *
     * <p>For block 0 (first-ever wrap), the check is skipped because there is no
     * previous block to compare against.
     *
     * @param firstNewBlock the block number of the first newly wrapped block
     * @param expectedPreviousHash the expected previous block root hash (from the pre-existing chain)
     * @throws Exception if the boundary hash does not match or the block cannot be read
     */
    private void validateBoundary(long firstNewBlock, byte[] expectedPreviousHash) throws Exception {
        if (firstNewBlock == 0) {
            // First-ever wrap: no previous block to compare against
            return;
        }
        if (expectedPreviousHash == null) {
            throw new IllegalStateException(
                    "No previous block hash available for boundary check at block " + firstNewBlock);
        }

        final Block block = BlockReader.readBlock(outputBlocksDir, firstNewBlock);
        byte[] actualPreviousHash = null;
        for (final BlockItem item : block.items()) {
            if (item.hasBlockFooter()) {
                actualPreviousHash = item.blockFooter().previousBlockRootHash().toByteArray();
                break;
            }
        }

        if (actualPreviousHash == null) {
            throw new IllegalStateException(
                    "Block " + firstNewBlock + " has no BlockFooter — cannot verify boundary hash");
        }

        if (!Arrays.equals(expectedPreviousHash, actualPreviousHash)) {
            throw new IllegalStateException("Boundary hash mismatch at block " + firstNewBlock
                    + ": expected previousBlockRootHash=" + HexFormat.of().formatHex(expectedPreviousHash)
                    + " but found=" + HexFormat.of().formatHex(actualPreviousHash));
        }
    }

    /**
     * Rolls back a failed wrap by deleting entirely-new zip files, truncating the block
     * hash registry, and resetting the watermark and block counter.
     *
     * <p>Mixed zips (containing both old and new blocks) are intentionally left in place;
     * {@link #reconcileRegistryWithWatermark()} handles mid-zip cleanup on the next startup.
     *
     * @param preWrapHighestBlock the highest block number before the wrap started (-1 if none)
     * @param postWrapHighestBlock the highest block number after the (failed) wrap
     */
    private void rollbackDay(long preWrapHighestBlock, long postWrapHighestBlock) {
        System.err.println(
                "[WRAP] Rolling back: deleting new zips and truncating state to block " + preWrapHighestBlock);

        final long blocksPerZip = (long) Math.pow(10, DEFAULT_POWERS_OF_TEN_PER_ZIP);
        final long firstNewBlock = preWrapHighestBlock + 1;
        final long firstNewZipStart = BlockWriter.zipRangeFirstBlock(firstNewBlock, DEFAULT_POWERS_OF_TEN_PER_ZIP);
        // Only delete zips that are entirely new — skip the mixed zip (if any)
        final long deleteStartBlock =
                (firstNewZipStart == firstNewBlock) ? firstNewZipStart : firstNewZipStart + blocksPerZip;

        for (long zipStart = deleteStartBlock; zipStart <= postWrapHighestBlock; zipStart += blocksPerZip) {
            final BlockPath blockPath =
                    BlockWriter.computeBlockPath(outputBlocksDir, zipStart, BlockArchiveType.UNCOMPRESSED_ZIP);
            try {
                if (Files.deleteIfExists(blockPath.zipFilePath())) {
                    System.err.println("[WRAP]   Deleted " + blockPath.zipFilePath());
                }
            } catch (IOException e) {
                System.err.println(
                        "[WRAP]   Warning: could not delete " + blockPath.zipFilePath() + ": " + e.getMessage());
            }
        }

        // Truncate registry and reset counters
        blockRegistry.truncateTo(preWrapHighestBlock);
        durableWatermark.set(preWrapHighestBlock);
        blockCounter.set(preWrapHighestBlock + 1);
        if (preWrapHighestBlock >= 0) {
            saveWatermark(watermarkFile, preWrapHighestBlock);
        } else {
            try {
                Files.deleteIfExists(watermarkFile);
            } catch (IOException e) {
                System.err.println("[WRAP]   Warning: could not delete watermark file: " + e.getMessage());
            }
        }
    }

    /**
     * Reconcile block hash registry with durable watermark on startup.
     * If registry is ahead of watermark, truncate it. Handle mid-zip resume.
     */
    private void reconcileRegistryWithWatermark() throws IOException {
        final long watermark = durableWatermark.get();
        final long registryHighest = blockRegistry.highestBlockNumberStored();

        if (watermark >= 0 && registryHighest > watermark) {
            System.out.println("[WRAP] Registry has blocks up to " + registryHighest + " but watermark is at "
                    + watermark + "; truncating registry.");
            blockRegistry.truncateTo(watermark);
        } else if (watermark < 0 && registryHighest >= 0) {
            System.out.println("[WRAP] No watermark file found; trusting registry at block " + registryHighest);
            durableWatermark.set(registryHighest);
        }

        long effectiveHighest = blockRegistry.highestBlockNumberStored();

        // Mid-zip resume detection
        if (effectiveHighest >= 0) {
            final long zipRangeFirst = BlockWriter.zipRangeFirstBlock(effectiveHighest, DEFAULT_POWERS_OF_TEN_PER_ZIP);
            final long blocksPerZip = (long) Math.pow(10, DEFAULT_POWERS_OF_TEN_PER_ZIP);
            final long zipRangeLast = zipRangeFirst + blocksPerZip - 1;
            if (effectiveHighest < zipRangeLast) {
                final long truncateTo = zipRangeFirst - 1;
                System.out.println("[WRAP] Mid-zip resume: block " + effectiveHighest + " is inside zip range "
                        + zipRangeFirst + "-" + zipRangeLast + "; truncating to " + truncateTo
                        + " and deleting partial zip.");
                final BlockPath partialZipPath = BlockWriter.computeBlockPath(
                        outputBlocksDir, effectiveHighest, BlockArchiveType.UNCOMPRESSED_ZIP);
                Files.deleteIfExists(partialZipPath.zipFilePath());
                blockRegistry.truncateTo(truncateTo);
                durableWatermark.set(truncateTo);
                saveWatermark(watermarkFile, truncateTo);
            }
        }
    }

    /**
     * Discovers address book updates from the block's record stream transactions and,
     * if the address book changed, re-verifies signatures with the correct keys.
     */
    private PreVerifiedBlock updateAddressBookAndReverify(final PreVerifiedBlock preVerified, final long blockNum) {
        final List<RecordStreamItem> streamItems =
                preVerified.recordBlock().recordFile().recordStreamFile().recordStreamItems();
        final List<Transaction> transactions = new ArrayList<>();
        for (final RecordStreamItem rsi : streamItems) {
            if (rsi.hasTransaction()) {
                transactions.add(rsi.transactionOrThrow());
            }
        }

        if (!transactions.isEmpty()) {
            try {
                final List<TransactionBody> addressBookTxns =
                        AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
                if (!addressBookTxns.isEmpty()) {
                    final Instant blockInstant = preVerified.recordBlock().blockTime();
                    final String changes =
                            addressBookRegistry.updateAddressBook(blockInstant.plusNanos(1), addressBookTxns);
                    if (changes != null) {
                        System.out.println("[WRAP] Address book updated at block " + blockNum + ": " + changes);
                    }
                }
            } catch (Exception e) {
                // Don't fail wrapping for address book parse errors
            }
        }

        final NodeAddressBook currentBook = addressBookRegistry.getAddressBookForBlock(
                preVerified.recordBlock().blockTime());
        if (!currentBook.equals(preVerified.addressBook())) {
            final byte[] signedHash = preVerified.recordBlock().recordFile().signedHash();
            final List<RecordFileSignature> reverifiedSigs = preVerified.recordBlock().signatureFiles().stream()
                    .filter(psf -> psf.isValid(signedHash, currentBook))
                    .map(psf -> psf.toRecordFileSignature(currentBook))
                    .toList();
            System.out.printf(
                    "[WRAP] Block %d: re-verified signatures with updated address book: %d verified (was %d)%n",
                    blockNum,
                    reverifiedSigs.size(),
                    preVerified.verifiedSignatures().size());
            return new PreVerifiedBlock(preVerified.recordBlock(), currentBook, reverifiedSigs);
        }

        return preVerified;
    }

    /**
     * Load the durable commit watermark.
     */
    static long loadWatermark(Path watermarkFile) {
        if (!Files.exists(watermarkFile)) {
            return -1;
        }
        try {
            final byte[] bytes = Files.readAllBytes(watermarkFile);
            if (bytes.length < Long.BYTES) {
                return -1;
            }
            return ByteBuffer.wrap(bytes).getLong();
        } catch (IOException e) {
            System.err.println("[WRAP] Warning: could not read watermark file: " + e.getMessage());
            return -1;
        }
    }

    /**
     * Save the durable commit watermark atomically.
     */
    static void saveWatermark(Path watermarkFile, long blockNumber) {
        if (blockNumber < 0) {
            return;
        }
        final Path tmpFile = watermarkFile.resolveSibling(WATERMARK_FILE_NAME + ".tmp");
        try {
            final byte[] bytes =
                    ByteBuffer.allocate(Long.BYTES).putLong(blockNumber).array();
            Files.write(tmpFile, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile, watermarkFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("[WRAP] Warning: could not save watermark: " + e.getMessage());
        }
    }

    /**
     * Validates a completed day archive for completeness before wrapping.
     *
     * <p>Checks:
     * <ul>
     *   <li>Block count matches expected range from dayBlockInfo</li>
     *   <li>Archive can be read without errors</li>
     * </ul>
     *
     * @param dayArchivePath path to the .tar.zstd day archive
     * @param dayBlockInfo expected block info for this day
     * @throws IllegalStateException if validation fails (fail-hard)
     */
    public static void validateDayArchive(Path dayArchivePath, DayBlockInfo dayBlockInfo) {
        long expectedBlocks = dayBlockInfo.lastBlockNumber - dayBlockInfo.firstBlockNumber + 1;
        long actualBlocks = 0;

        try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(dayArchivePath)) {
            actualBlocks = stream.count();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "FATAL: Day archive " + dayArchivePath.getFileName() + " is corrupt and cannot be read: "
                            + e.getMessage()
                            + "\nPlease delete the archive and re-run to re-download this day.",
                    e);
        }

        if (actualBlocks != expectedBlocks) {
            throw new IllegalStateException(
                    "FATAL: Day archive " + dayArchivePath.getFileName() + " is incomplete: expected " + expectedBlocks
                            + " blocks but found " + actualBlocks + "."
                            + "\nThis is likely caused by GCP cache/listing inconsistencies."
                            + "\nPlease delete the archive and re-run to re-download this day.");
        }
    }
}
