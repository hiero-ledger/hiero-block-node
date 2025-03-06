// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.simulator.Constants.GZ_EXTENSION;
import static org.hiero.block.simulator.Constants.RECORD_EXTENSION;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;

/**
 * The BlockAsDirBlockStreamManager class implements the BlockStreamManager interface to manage the
 * block stream from a directory.
 */
public class BlockAsDirBlockStreamManager implements BlockStreamManager {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    final String rootFolder;

    final List<Block> blocks = new ArrayList<>();

    int currentBlockIndex = 0;
    int currentBlockItemIndex = 0;
    int lastGivenBlockNumber = 0;

    /**
     * Constructor to initialize the BlockAsDirBlockStreamManager with the block stream
     * configuration.
     *
     * @param blockGeneratorConfig the block stream configuration
     */
    @Inject
    public BlockAsDirBlockStreamManager(@NonNull BlockGeneratorConfig blockGeneratorConfig) {
        this.rootFolder = blockGeneratorConfig.folderRootPath();
    }

    /**
     * Initialize the block stream manager and load blocks into memory.
     */
    @Override
    public void init() {
        try {
            this.loadBlocks();
        } catch (IOException | ParseException | IllegalArgumentException e) {
            LOGGER.log(ERROR, "Error loading blocks", e);
            throw new RuntimeException(e);
        }

        LOGGER.log(INFO, "Loaded " + blocks.size() + " blocks into memory");
    }

    /** Generation Mode of the implementation */
    @Override
    public GenerationMode getGenerationMode() {
        return GenerationMode.DIR;
    }

    /** gets the next block item from the manager */
    @Override
    public BlockItem getNextBlockItem() {
        BlockItem nextBlockItem = blocks.get(currentBlockIndex).getItemsList().get(currentBlockItemIndex);
        currentBlockItemIndex++;
        if (currentBlockItemIndex
                >= blocks.get(currentBlockIndex).getItemsList().size()) {
            currentBlockItemIndex = 0;
            currentBlockIndex++;
            if (currentBlockIndex >= blocks.size()) {
                currentBlockIndex = 0;
            }
        }
        return nextBlockItem;
    }

    /** gets the next block from the manager */
    @Override
    public Block getNextBlock() {
        Block nextBlock = blocks.get(currentBlockIndex);
        currentBlockIndex++;
        lastGivenBlockNumber++;
        if (currentBlockIndex >= blocks.size()) {
            currentBlockIndex = 0;
        }
        return nextBlock;
    }

    private void loadBlocks() throws IOException, ParseException {
        final Path rootPath = Path.of(rootFolder);

        try (final Stream<Path> blockDirs = Files.list(rootPath).filter(Files::isDirectory)) {
            final List<Path> sortedBlockDirs =
                    blockDirs.sorted(Comparator.comparing(Path::getFileName)).toList();

            for (final Path blockDirPath : sortedBlockDirs) {
                final List<BlockItem> parsedBlockItems = new ArrayList<>();

                try (final Stream<Path> blockItems = Files.list(blockDirPath).filter(Files::isRegularFile)) {
                    final Comparator<Path> comparator =
                            Comparator.comparing(BlockAsDirBlockStreamManager::extractNumberFromPath);
                    final List<Path> sortedBlockItems =
                            blockItems.sorted(comparator).toList();

                    for (final Path pathBlockItem : sortedBlockItems) {
                        final byte[] blockItemBytes =
                                FileUtilities.readFileBytesUnsafe(pathBlockItem, RECORD_EXTENSION, GZ_EXTENSION);
                        // if null means the file is not a block item and we can skip the file.
                        if (blockItemBytes == null) {
                            continue;
                        }
                        final BlockItem blockItem = BlockItem.parseFrom(blockItemBytes);
                        parsedBlockItems.add(blockItem);
                    }
                }

                blocks.add(Block.newBuilder().addAllItems(parsedBlockItems).build());
                LOGGER.log(DEBUG, "Loaded block: " + blockDirPath);
            }
        }
    }

    // Method to extract the numeric part of the filename from a Path object
    // Returns -1 if the filename is not a valid number
    private static int extractNumberFromPath(Path path) {
        String filename = path.getFileName().toString();
        String numPart = filename.split("\\.")[0]; // Get the part before the first dot
        try {
            return Integer.parseInt(numPart);
        } catch (NumberFormatException e) {
            return -1; // Return -1 if parsing fails
        }
    }
}
