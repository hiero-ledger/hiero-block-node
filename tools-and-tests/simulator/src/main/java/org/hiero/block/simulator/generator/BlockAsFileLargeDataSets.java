// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.simulator.Constants.GZ_EXTENSION;
import static org.hiero.block.simulator.Constants.RECORD_EXTENSION;

import com.hedera.hapi.block.stream.protoc.Block;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
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
    private long currentBlockNumber;
    private final int endBlockNumber;

    private final String formatString;
    private final Set<Long> blockNumbers = new HashSet<>();

    /**
     * Constructs a new BlockAsFileLargeDataSets instance.
     *
     * @param config the block stream configuration
     */
    @Inject
    public BlockAsFileLargeDataSets(@NonNull BlockGeneratorConfig config) {

        this.blockStreamPath = config.folderRootPath();
        this.endBlockNumber = config.endBlockNumber();
        this.currentBlockNumber = config.startBlockNumber();

        this.formatString = "%0" + config.paddedLength() + "d" + config.fileExtension();
    }

    @Override
    public void init() {
        try (var stream = Files.walk(Path.of(blockStreamPath))) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(GZ_EXTENSION))
                    .forEach(blockFile -> blockNumbers.add(blockNumberFromFile(blockFile)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenerationMode getGenerationMode() {
        return GenerationMode.DIR;
    }

    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException {
        if (blockNumbers.isEmpty()) {
            return null;
        }
        // If endBlockNumber is set, evaluate if we've exceeded the
        // range. If so, then return null.
        if (endBlockNumber > 0 && currentBlockNumber > endBlockNumber) {
            return null;
        }

        long nextBlockNumber = blockNumbers.iterator().next();
        blockNumbers.remove(nextBlockNumber);

        final String nextBlockFileName = String.format(formatString, nextBlockNumber);
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

        currentBlockNumber = nextBlockNumber;

        return block;
    }

    @Override
    public void resetToBlock(final long block) {
        currentBlockNumber = block;
    }

    /**
     * Extracts the block number from a file name. The file name is expected to be in the format
     * {@code "0000000000000000000.blk.xyz"} where the block number is the first 19 digits and the rest is the file
     * extension.
     *
     * @param fileName the file name to extract the block number from
     * @return the block number
     */
    public static long blockNumberFromFile(final String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    }

    /**
     * Extracts the block number from a file name. The file name is expected to be in the format
     * {@code "0000000000000000000.blk.xyz"} where the block number is the first 19 digits and the rest is the file
     * extension.
     *
     * @param file the path for file to extract the block number from
     * @return the block number
     */
    public static long blockNumberFromFile(final Path file) {
        return blockNumberFromFile(file.getFileName().toString());
    }
}
