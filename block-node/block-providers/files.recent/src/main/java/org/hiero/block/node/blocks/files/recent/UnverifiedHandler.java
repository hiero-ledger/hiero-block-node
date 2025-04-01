// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.hiero.block.node.base.BlockFile.nestedDirectoriesAllBlockNumbers;

import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongConsumer;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.node.base.BlockFile;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;

/**
 * This class is responsible for handling unverified blocks. Storing them till they are verified if needed. Avoiding
 * race conditions between the block verification notifications and incoming blocks.
 */
final class UnverifiedHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The configuration for this plugin. */
    private final FilesRecentConfig config;
    /** Stack of the last 100 verified block numbers. */
    private final LinkedList<Long> last100VerifiedBlockNumbers = new LinkedList<>();
    /** List of all completed unverified blocks stored on disk */
    private final Set<Long> completedUnverifiedBlocks;
    /** Function to call to move the block to live storage */
    private final LongConsumer moveToLive;

    /**
     * Constructor for the UnverifiedHandler.
     *
     * @param config the configuration for this plugin
     * @param moveToLive function to call to move the block to live storage
     */
    UnverifiedHandler(@NonNull final FilesRecentConfig config, @NonNull final LongConsumer moveToLive) {
        this.config = Objects.requireNonNull(config);
        this.moveToLive = Objects.requireNonNull(moveToLive);
        completedUnverifiedBlocks = nestedDirectoriesAllBlockNumbers(config.liveRootPath(), config.compression());
    }

    /**
     * Called when a block is verified.
     *
     * @param blockNumber the block number of the verified block
     */
    synchronized void blockVerified(final long blockNumber) {
        // add the verified block number to the stack of completed unverified blocks
        last100VerifiedBlockNumbers.add(blockNumber);
        while (last100VerifiedBlockNumbers.size() > 100) {
            last100VerifiedBlockNumbers.removeFirst();
        }
        // check if the block number is in the completed unverified blocks
        if (completedUnverifiedBlocks.contains(blockNumber)) {
            // remove the block number from the completed unverified blocks
            completedUnverifiedBlocks.remove(blockNumber);
            // move the block to the live storage
            moveToLive.accept(blockNumber);
        } else {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Block {0} is verified but not found in unverified storage",
                    blockNumber);
            throw new IllegalStateException(
                    "Block %d is verified but not found in unverified storage".formatted(blockNumber));
        }
    }

    /**
     * If the block has not been verified yet, store it in the unverified block storage and return true. If the block
     * has already been verified, do not store it and return false.
     *
     * @param blockItems the block items representing the block to store
     * @param blockNumber the block number of the block to store
     * @return true if the block was stored, false if it was already verified
     */
    synchronized boolean storeIfUnverifiedBlock(
            @NonNull final List<BlockItemUnparsed> blockItems, final long blockNumber) {
        Objects.requireNonNull(blockItems);
        // check if the block number is already verified
        if (last100VerifiedBlockNumbers.contains(blockNumber)) {
            // block is already verified, do not store it
            return false;
        }
        // create the file for the block
        final Path unverifiedBlockPath =
                BlockFile.standaloneBlockFilePath(config.unverifiedRootPath(), blockNumber, config.compression());
        // create parent directory if it does not exist
        try {
            Files.createDirectories(unverifiedBlockPath.getParent(), FileUtilities.DEFAULT_FOLDER_PERMISSIONS);
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to create unverified block directory: {0}, error: {1}",
                    unverifiedBlockPath.getParent().toAbsolutePath().toString(),
                    e.getMessage());
            throw new RuntimeException(e);
        }
        // write the block to the file
        try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
                config.compression().wrapStream(Files.newOutputStream(unverifiedBlockPath)), 1024 * 1024))) {
            // write the block items to the file
            BlockUnparsed.PROTOBUF.write(new BlockUnparsed(blockItems), streamingData);
            streamingData.flush();
            LOGGER.log(
                    Level.DEBUG,
                    "Wrote unverified block: {0} to file: {1}",
                    blockNumber,
                    unverifiedBlockPath.toAbsolutePath().toString());
            // add to the completed unverified blocks
            completedUnverifiedBlocks.add(blockNumber);
            // return true to indicate that the block was stored
            return true;
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to write unverified block {0} to file: {1}, error: {2}",
                    blockNumber,
                    unverifiedBlockPath.toAbsolutePath().toString(),
                    e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
