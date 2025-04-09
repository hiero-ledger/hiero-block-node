// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.hiero.block.node.base.BlockFile.nestedDirectoriesAllBlockNumbers;

import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
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
public final class BlocksFilesRecentPlugin implements BlockProviderPlugin, BlockNotificationHandler, BlockItemHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The configuration for this plugin. */
    private FilesRecentConfig config;
    /** The block messaging facility. */
    private BlockMessagingFacility blockMessaging;
    /** The set of available blocks. */
    private final ConcurrentLongRangeSet availableBlocks = new ConcurrentLongRangeSet();
    /** The handler for unverified blocks. */
    private UnverifiedHandler unverifiedHandler;

    /**
     * Default constructor for the plugin. This is used for normal service loading.
     */
    public BlocksFilesRecentPlugin() {}

    /**
     * Constructor for the plugin. This is used for testing.
     *
     * @param config the config to use
     */
    BlocksFilesRecentPlugin(FilesRecentConfig config) {
        this.config = config;
    }

    // ==== BlockProviderPlugin Methods ================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(FilesRecentConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        // load config if not already set by test
        if (this.config == null) {
            this.config = context.configuration().getConfigData(FilesRecentConfig.class);
        }
        this.blockMessaging = context.blockMessaging();
        // create plugin data root directory if it does not exist
        try {
            Files.createDirectories(config.liveRootPath());
            Files.createDirectories(config.unverifiedRootPath());
        } catch (final IOException e) {
            LOGGER.log(Level.ERROR, "Could not create root directory", e);
            context.serverHealth().shutdown(name(), "Could not create root directory");
        }
        // create the unverified handler
        unverifiedHandler = new UnverifiedHandler(config, this::moveFileToLiveStorage);
        // we want to listen to incoming block items and write them into files in this plugins storage
        context.blockMessaging().registerBlockItemHandler(this, false, "BlocksFilesRecent");
        // we want to listen to block notifications and to know when blocks are verified
        context.blockMessaging().registerBlockNotificationHandler(this, false, "BlocksFilesRecent");
        // on start-up we can clear the unverified path as all unverified blocks will have to be resent
        try (final Stream<Path> stream = Files.walk(config.unverifiedRootPath(), 1)) {
            // TODO check it is not a directory abd us a block file, ie. don't delete files if dir is "/" :-)
            stream.filter(Files::isRegularFile).forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (final IOException e) {
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Failed to delete unverified file: %s, error: %s",
                            file.toString(),
                            e.getMessage());
                    throw new UncheckedIOException(e);
                }
            });
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to delete unverified files in path: %s, error: %s",
                    config.unverifiedRootPath(),
                    e.getMessage());
            context.serverHealth().shutdown(BlocksFilesRecentPlugin.class.getName(), e.getMessage());
        }
        // scan file system to find the oldest and newest blocks
        // TODO this can be way for efficient, very brute force at the moment
        nestedDirectoriesAllBlockNumbers(config.liveRootPath(), config.compression())
                .forEach(availableBlocks::add);
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
    public BlockAccessor block(final long blockNumber) {
        if (availableBlocks.contains(blockNumber)) {
            // we should have this block stored so go file the file and return accessor to it
            final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                    config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
            if (Files.exists(verifiedBlockPath)) {
                // we have the block so return it
                return new BlockFileBlockAccessor(config.liveRootPath(), verifiedBlockPath, config.compression());
            } else {
                LOGGER.log(
                        Level.WARNING,
                        "Failed to find verified block file: {0}",
                        verifiedBlockPath.toAbsolutePath().toString());
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public BlockRangeSet availableBlocks() {
        return availableBlocks;
    }

    // ==== BlockNotificationHandler Methods ===========================================================================

    /**
     * {@inheritDoc}
     * <p>
     * This method is called when a block notification is received. It is called on the block item notification thread.
     */
    @Override
    public void handleBlockNotification(final BlockNotification notification) {
        if (notification.type() == BlockNotification.Type.BLOCK_VERIFIED) {
            unverifiedHandler.blockVerified(notification.blockNumber());
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
    public void handleBlockItemsReceived(final BlockItems blockItems) {
        if (currentIncomingBlockNumber == UNKNOWN_BLOCK_NUMBER) {
            // we are not in any block, so check if this is the start of a new block
            if (blockItems.isStartOfNewBlock()) {
                // we are starting a new block, so set the current block number
                currentIncomingBlockNumber = blockItems.newBlockNumber();
                // double check that we are not in the middle of a block and previous block was closed
                if (!currentBlocksItems.isEmpty()) {
                    LOGGER.log(Level.ERROR, "Previous block was not complete, block: {0}", currentIncomingBlockNumber);
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
            if (!unverifiedHandler.storeIfUnverifiedBlock(currentBlocksItems, currentIncomingBlockNumber)) {
                // the block is already verified, so we can just write the block to the live path
                writeBlockToLivePath(currentBlocksItems, currentIncomingBlockNumber);
            }
            // so we are done with block so clear the current block items and block number
            currentBlocksItems.clear();
            currentIncomingBlockNumber = UNKNOWN_BLOCK_NUMBER;
        }
    }

    // ==== Action Methods =============================================================================================

    /**
     * Move an unverified block file to the live storage. This is called when the block is verified and has already been
     * written to unverified storage.
     *
     * @param blockNumber the block number of the block to move
     */
    private void moveFileToLiveStorage(final long blockNumber) {
        // we need to move it to the verified block storage
        final Path unverifiedBlockPath =
                BlockFile.standaloneBlockFilePath(config.unverifiedRootPath(), blockNumber, config.compression());
        final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
        try {
            // create parent directory if it does not exist
            if (verifiedBlockPath.getFileSystem().equals(FileSystems.getDefault())) {
                // we are using the default file system so we need to create the parent directory
                Files.createDirectories(verifiedBlockPath.getParent(), FileUtilities.DEFAULT_FOLDER_PERMISSIONS);
            } else {
                Files.createDirectories(verifiedBlockPath.getParent());
            }
            // move the file
            Files.move(unverifiedBlockPath, verifiedBlockPath, StandardCopyOption.ATOMIC_MOVE);
            // update the oldest and newest verified block numbers
            availableBlocks.add(blockNumber);
            LOGGER.log(Level.DEBUG, "Moved block: {0} from Unverified to Verified", blockNumber);
            // send block persisted notification
            blockMessaging.sendBlockNotification(
                    new BlockNotification(blockNumber, BlockNotification.Type.BLOCK_PERSISTED, null));
        } catch (final IOException e) {
            LOGGER.log(
                    Level.ERROR,
                    "Failed to move unverified file: {0} to verified path: {1}, error: {2}",
                    unverifiedBlockPath.toAbsolutePath().toString(),
                    verifiedBlockPath.toAbsolutePath().toString(),
                    e.getMessage());
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Directly write a block to verified storage. This is used when the block is already verified when we receive it.
     *
     * @param blockItems the block items representing block to write
     * @param blockNumber the block number of the block to write
     */
    private void writeBlockToLivePath(final List<BlockItemUnparsed> blockItems, final long blockNumber) {
        final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
        try {
            // create parent directory if it does not exist
            Files.createDirectories(verifiedBlockPath.getParent());
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to create directories for path: {0} error: {1}",
                    verifiedBlockPath.toAbsolutePath().toString(),
                    e);
            throw new UncheckedIOException(e);
        }
        try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
                config.compression().wrapStream(Files.newOutputStream(verifiedBlockPath)), 1024 * 1024))) {
            BlockUnparsed.PROTOBUF.write(new BlockUnparsed(blockItems), streamingData);
            streamingData.flush();
            LOGGER.log(
                    Level.DEBUG,
                    "Wrote verified block: {0} to file: {1}",
                    blockNumber,
                    verifiedBlockPath.toAbsolutePath().toString());
            // update the oldest and newest verified block numbers
            availableBlocks.add(blockNumber);
            // send block persisted notification
            blockMessaging.sendBlockNotification(
                    new BlockNotification(blockNumber, BlockNotification.Type.BLOCK_PERSISTED, null));
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to create verified file for block: {0}, error: {1}",
                    blockNumber,
                    e);
            throw new UncheckedIOException(e);
        }
    }
}
