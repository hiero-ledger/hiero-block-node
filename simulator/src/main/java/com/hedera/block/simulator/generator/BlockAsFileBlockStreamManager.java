// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.generator;

import static com.hedera.block.simulator.Constants.GZ_EXTENSION;
import static com.hedera.block.simulator.Constants.RECORD_EXTENSION;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

import com.hedera.block.simulator.config.data.BlockGeneratorConfig;
import com.hedera.block.simulator.config.types.GenerationMode;
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

/** The block as file block stream manager. */
public class BlockAsFileBlockStreamManager implements BlockStreamManager {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    final String rootFolder;

    final List<Block> blocks = new ArrayList<>();

    int currentBlockIndex = 0;
    int currentBlockItemIndex = 0;
    int lastGivenBlockNumber = 0;

    /**
     * Constructor for the block as file block stream manager.
     *
     * @param blockStreamConfig the block stream config
     */
    @Inject
    public BlockAsFileBlockStreamManager(@NonNull BlockGeneratorConfig blockStreamConfig) {
        this.rootFolder = blockStreamConfig.folderRootPath();
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

    @Override
    public GenerationMode getGenerationMode() {
        return GenerationMode.DIR;
    }

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

        try (final Stream<Path> blockFiles = Files.list(rootPath)) {

            final List<Path> sortedBlockFiles =
                    blockFiles.sorted(Comparator.comparing(Path::getFileName)).toList();

            for (final Path blockPath : sortedBlockFiles) {

                final byte[] blockBytes = FileUtilities.readFileBytesUnsafe(blockPath, RECORD_EXTENSION, GZ_EXTENSION);
                // skip if block is null, usually due to SO files like .DS_STORE
                if (blockBytes == null) {
                    continue;
                }

                final Block block = Block.parseFrom(blockBytes);
                blocks.add(block);
                LOGGER.log(DEBUG, "Loaded block: " + blockPath);
            }
        }
    }
}
