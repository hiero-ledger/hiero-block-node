// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.hiero.block.node.base.BlockFile.nestedDirectoriesMaxBlockNumber;
import static org.hiero.block.node.base.BlockFile.nestedDirectoriesMinBlockNumber;

import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;

/**
 * This plugin is responsible for providing the "Files Recent" block provider. This stores incoming blocks in files in
 * the local filesystem. It stores block items as soon as they are received into a temporary file until the ends of the
 * block. The temporary file is stored in unverified path. Once the block is verified, it is moved to the live path.
 * This plugin assumes that it stores blocks forever until asked to delete them.
 * <h2>Threading</h2>
 * There are three threading interactions for this class. Any shared state between the three interactions needs to be
 * considered multithreaded, so handled with thread safe data structures.
 * <ul>
 *     <li><b>BlockProviderPlugin methods</b> - The init() and start() methods are called at startup only and only ever
 *     by one thread at a time and before any listeners are called. The reading methods block() and latestBlockNumber()
 *     need to be handled in a thread safe way. As they can be called on any thread. So any state accessed needs to be
 *     final or thread safe data structures.</li>
 *     <li><b>BlockNotificationHandler methods</b> - These are always called on the same single dedicated thread for
 *     this handler.</li>
 *     <li><b>BlockItemHandler methods</b> - These are always called on the same single dedicated thread for this
 *     handler.It should do all work on that thread and block it till work is done. By doing that it will provide back
 *     pressure into the messaging system. This is important as it stops the messaging system running ahead of the
 *     plugin resulting in missing block item chucks. If this plugin can not keep up with the incoming block items rate,
 *     the messaging system will provide back pressure through the provider to the consensus nodes pushing block items
 *     to the block node.</li>
 * </ul>
 * <h2>Unverified Blocks</h2>
 * The storage of unverified blocks is done in a configured directory. That directory can be in temporary storage as it
 * is not required to be persistent. On start-up, the plugin will delete any files in the unverified directory. This is
 * done to clean up any files that are left over from a previous run. The unverified directory does not have any special
 * subdirectory structure and blocks are just stored as individual files directly in that directory. This is fine as
 * there should never be more than a few unverified blocks at a time. The unverified blocks are stored in a compressed
 * format so they are ready to just be moved to the live directory when they are verified. The compression type is
 * configured and can be changed at any time. The compression level is also configured and can be changed at any time.
 */
public class BlocksFilesRecentPlugin implements BlockProviderPlugin, BlockNotificationHandler, BlockItemHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context. */
    private BlockNodeContext context;
    /** The configuration for this plugin. */
    private FilesRecentConfig config;
    /** The block number of the oldest verified block, this is inclusive. */
    private final AtomicLong oldestVerifiedBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    /** The block number of the newest verified block, this is also inclusive. */
    private final AtomicLong newestVerifiedBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    /** List of all completed unverified blocks stored on disk */
    private final ConcurrentSkipListSet<Long> completedUnverifiedBlocks = new ConcurrentSkipListSet<>();

    // ==== BlockProviderPlugin Methods ================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "Files Recent";
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public Builder<?, ? extends Routing> init(BlockNodeContext context) {
        this.context = context;
        this.config = context.configuration().getConfigData(FilesRecentConfig.class);
        // we want to listen to incoming block items and write them into files in this plugins storage
        context.blockMessaging().registerBlockItemHandler(this, false, "BlocksFilesRecent");
        // we want to listen to block notifications and to know when blocks are verified
        context.blockMessaging().registerBlockNotificationHandler(this, false, "BlocksFilesRecent");
        // on start-up we can clear the unverified path as all unverified blocks will have to be resent
        try (final var stream = Files.walk(config.unverifiedRootPath(), 1)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Failed to delete unverified file: %s, error: %s",
                            file.toString(),
                            e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to delete unverified files in path: %s, error: %s",
                    config.unverifiedRootPath(),
                    e.getMessage());
            context.serverHealth().shutdown(BlocksFilesRecentPlugin.class.getName(), e.getMessage());
        }
        // scan file system to find the oldest and newest blocks
        oldestVerifiedBlockNumber.set(nestedDirectoriesMinBlockNumber(config.liveRootPath(), config.compression()));
        newestVerifiedBlockNumber.set(nestedDirectoriesMaxBlockNumber(config.liveRootPath(), config.compression()));
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int defaultPriority() {
        return 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * We only provide read access to verified blocks.
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        if (blockNumber >= oldestVerifiedBlockNumber.get() || blockNumber <= newestVerifiedBlockNumber.get()) {
            // we should have this block stored so go file the file and return accessor to it
            final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                    config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
            if (Files.exists(verifiedBlockPath)) {
                // we have the block so return it
                return new BlockFileBlockAccessor(config.liveRootPath(), verifiedBlockPath, config.compression());
            } else {
                LOGGER.log(
                        Level.WARNING,
                        "Failed to find verified block file: %s",
                        verifiedBlockPath.toAbsolutePath().toString());
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long latestBlockNumber() {
        return newestVerifiedBlockNumber.get();
    }

    // ==== BlockNotificationHandler Methods ===========================================================================

    /**
     * {@inheritDoc}
     * <p>
     * This method is called when a block notification is received. It is called on the block item notification thread.
     */
    @Override
    public void handleBlockNotification(BlockNotification notification) {
        if (notification.type() == BlockNotification.Type.BLOCK_VERIFIED) {
            // we have a verified block
            final long blockNumber = notification.blockNumber();
            // check we have that block in unverified storage
            if (completedUnverifiedBlocks.contains(blockNumber)) {
                // we need to move it to the verified block storage
                final Path unverifiedBlockPath = BlockFile.standaloneBlockFilePath(
                        config.unverifiedRootPath(), blockNumber, config.compression());
                final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                        config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
                try {
                    // create parent directory if it does not exist
                    Files.createDirectories(verifiedBlockPath.getParent(), FileUtilities.DEFAULT_FOLDER_PERMISSIONS);
                    // create move file
                    Files.move(unverifiedBlockPath, verifiedBlockPath, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Failed to move unverified file: %s to verified path: %s, error: %s",
                            unverifiedBlockPath.toAbsolutePath().toString(),
                            verifiedBlockPath.toAbsolutePath().toString(),
                            e.getMessage());
                    // TODO is there more that should be done here?
                }
                // we need to update the oldest and newest verified block numbers
                oldestVerifiedBlockNumber.compareAndSet(UNKNOWN_BLOCK_NUMBER, blockNumber);
                newestVerifiedBlockNumber.getAndUpdate(
                        existingBlockNumber -> Math.max(existingBlockNumber, blockNumber));
                // send notification that the block is verified
                context.blockMessaging()
                        .sendBlockNotification(
                                new BlockNotification(blockNumber, BlockNotification.Type.BLOCK_VERIFIED, null));
            } else {
                LOGGER.log(Level.WARNING, "Block %d is verified but not found in unverified storage", blockNumber);
            }
        }
    }

    // ==== BlockItemHandler State & Methods ===========================================================================

    /**
     * The block number of the current incoming block. This is set when the start of a new block is received. It is only
     * ever accessed on the block item handler thread.
     */
    private long currentIncomingBlockNumber = UNKNOWN_BLOCK_NUMBER;

    /**
     * The list of block items for the current incoming block. This is cleared when the start of a new block is
     * received, then new items appended till we get end of block proof. It is only ever accessed on the block item
     * handler thread.
     */
    private final List<BlockItemUnparsed> currentBlocksItems = new ArrayList<>();

    /**
     * {@inheritDoc}
     * <p>
     * Called when the block node receives new block items, this is called on the block item handler thread.
     */
    @Override
    public void handleBlockItemsReceived(BlockItems blockItems) {
        if (currentIncomingBlockNumber == UNKNOWN_BLOCK_NUMBER) {
            // we are not in any block, so check if this is the start of a new block
            if (blockItems.isStartOfNewBlock()) {
                // we are starting a new block, so set the current block number
                currentIncomingBlockNumber = blockItems.newBlockNumber();
                // double check that we are not in the middle of a block and previous block was closed
                if (!currentBlocksItems.isEmpty()) {
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Previous block was not complete, block: %d",
                            currentIncomingBlockNumber);
                    currentBlocksItems.clear();
                }
            } else {
                // we are in the middle of a block so wait for the start of next clean block
                return;
            }
        }
        // append the block items to the current block items
        currentBlocksItems.addAll(blockItems.blockItems());
        // check if we are at the end of the block
        if (blockItems.isEndOfBlock()) {
            // create the file for the block
            final Path unverifiedBlockPath = BlockFile.standaloneBlockFilePath(
                    config.unverifiedRootPath(), currentIncomingBlockNumber, config.compression());
            // create parent directory if it does not exist
            try {
                Files.createDirectories(unverifiedBlockPath.getParent(), FileUtilities.DEFAULT_FOLDER_PERMISSIONS);
            } catch (IOException e) {
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "Failed to create unverified block directory: %s, error: %s",
                        unverifiedBlockPath.getParent().toAbsolutePath().toString(),
                        e.getMessage());
                // TODO is there a better way to handle this than shutting down the server?
                context.serverHealth().shutdown(BlocksFilesRecentPlugin.class.getName(), e.getMessage());
            }
            try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
                    config.compression().wrapStream(Files.newOutputStream(unverifiedBlockPath)), 1024 * 1024))) {
                BlockUnparsed.PROTOBUF.write(new BlockUnparsed(currentBlocksItems), streamingData);
                streamingData.flush();
                // add the block number to the completed unverified blocks
                completedUnverifiedBlocks.add(currentIncomingBlockNumber);
            } catch (IOException e) {
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "Failed to create unverified file for block: %d, error: %s",
                        currentIncomingBlockNumber,
                        e.getMessage());
                // TODO is there a better way to handle this than shutting down the server?
                context.serverHealth().shutdown(BlocksFilesRecentPlugin.class.getName(), e.getMessage());
            }
        }
    }
}
