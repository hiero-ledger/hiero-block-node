// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.simulator.Constants.GZ_EXTENSION;
import static org.hiero.block.simulator.Constants.RECORD_EXTENSION;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import javax.inject.Inject;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/** A block stream manager that reads blocks from files in a directory. */
public class BlockAsFileLargeDataSets implements BlockStreamManager {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // State for getNextBlock()
    private final String blockStreamPath;
    private int currentBlockIndex;
    private final int endBlockNumber;

    // State for getNextBlockItem()
    private int currentBlockItemIndex;
    private Block currentBlock;
    private final String formatString;

    /**
     * Constructs a new BlockAsFileLargeDataSets instance.
     *
     * @param config the block stream configuration
     */
    @Inject
    public BlockAsFileLargeDataSets(@NonNull BlockGeneratorConfig config) {

        this.blockStreamPath = config.folderRootPath();
        this.endBlockNumber = config.endBlockNumber();

        // Override if startBlockNumber is set
        this.currentBlockIndex = (config.startBlockNumber() > 1) ? config.startBlockNumber() : 1;

        this.formatString = "%0" + config.paddedLength() + "d" + config.fileExtension();
    }

    @Override
    public GenerationMode getGenerationMode() {
        return GenerationMode.DIR;
    }

    @Override
    public BlockItem getNextBlockItem() throws IOException, BlockSimulatorParsingException {
        if (currentBlock != null && currentBlock.getItemsList().size() > currentBlockItemIndex) {
            return currentBlock.getItemsList().get(currentBlockItemIndex++);
        } else {
            currentBlock = getNextBlock();
            if (currentBlock != null) {
                currentBlockItemIndex = 0; // Reset for new block
                return getNextBlockItem();
            }
        }

        return null; // No more blocks/items
    }

    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException {

        // If endBlockNumber is set, evaluate if we've exceeded the
        // range. If so, then return null.
        if (endBlockNumber > 0 && currentBlockIndex > endBlockNumber) {
            return null;
        }

        final String nextBlockFileName = String.format(formatString, currentBlockIndex);
        final Path localBlockStreamPath = Path.of(blockStreamPath).resolve(nextBlockFileName);
        if (!Files.exists(localBlockStreamPath)) {
            return null;
        }
        final byte[] blockBytes =
                FileUtilities.readFileBytesUnsafe(localBlockStreamPath, RECORD_EXTENSION, GZ_EXTENSION);

        if (Objects.isNull(blockBytes)) {
            throw new NullPointerException(
                    "Unable to read block file [%s]! Most likely not found with the extensions '%s' or '%s'"
                            .formatted(localBlockStreamPath, RECORD_EXTENSION, GZ_EXTENSION));
        }

        LOGGER.log(INFO, "Loading block: " + localBlockStreamPath.getFileName());

        final Block block = Block.parseFrom(blockBytes);
        LOGGER.log(INFO, "block loaded with items size= " + block.getItemsList().size());

        currentBlockIndex++;

        return block;
    }

    @Override
    public void resetToBlock(final long block) {
        currentBlockIndex = (int) block;
    }
}
