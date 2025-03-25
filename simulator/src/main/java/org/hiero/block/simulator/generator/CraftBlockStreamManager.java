// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.hiero.block.common.hasher.Hashes;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.common.utils.StringUtilities;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.itemhandler.BlockHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockProofHandler;
import org.hiero.block.simulator.generator.itemhandler.EventHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.EventTransactionHandler;
import org.hiero.block.simulator.generator.itemhandler.ItemHandler;
import org.hiero.block.simulator.generator.itemhandler.TransactionResultHandler;

/**
 * Implementation of BlockStreamManager that crafts blocks from scratch rather than reading from an existing stream.
 * This manager generates synthetic blocks with random events and transactions based on configured parameters.
 */
public class CraftBlockStreamManager implements BlockStreamManager {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Service
    private final Random random;

    // Configuration
    private final int minEventsPerBlock;
    private final int maxEventsPerBlock;
    private final int minTransactionsPerEvent;
    private final int maxTransactionsPerEvent;

    // State
    private final GenerationMode generationMode;
    private final byte[] previousStateRootHash;
    private byte[] previousBlockHash;
    private byte[] currentBlockHash;
    private long currentBlockNumber;
    private StreamingTreeHasher inputTreeHasher;
    private StreamingTreeHasher outputTreeHasher;

    /**
     * Constructs a new CraftBlockStreamManager with the specified configuration.
     *
     * @param blockGeneratorConfig Configuration parameters for block generation including event and transaction counts
     * @throws NullPointerException if blockGeneratorConfig is null
     */
    public CraftBlockStreamManager(@NonNull BlockGeneratorConfig blockGeneratorConfig) {
        final BlockGeneratorConfig blockGeneratorConfig1 = requireNonNull(blockGeneratorConfig);
        this.generationMode = blockGeneratorConfig1.generationMode();
        this.currentBlockNumber = blockGeneratorConfig1.startBlockNumber();
        this.minEventsPerBlock = blockGeneratorConfig1.minEventsPerBlock();
        this.maxEventsPerBlock = blockGeneratorConfig1.maxEventsPerBlock();
        this.minTransactionsPerEvent = blockGeneratorConfig1.minTransactionsPerEvent();
        this.maxTransactionsPerEvent = blockGeneratorConfig1.maxTransactionsPerEvent();

        this.random = new Random();
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];

        this.previousBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];

        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();

        LOGGER.log(INFO, "Block Stream Simulator will use Craft mode for block management.");

        final Path rootDataPath = Path.of("/opt/simulator/data");
        final Path latestAckBlockNumberPath = rootDataPath.resolve("latestAckBlockNumber");
        final Path latestAckBlockHashPath = rootDataPath.resolve("latestAckBlockHash");
        if (Files.exists(latestAckBlockNumberPath)) {
            try {
                final String fromFile = Files.readString(latestAckBlockNumberPath);
                if (!StringUtilities.isBlank(fromFile)) {
                    currentBlockNumber = Long.parseLong(fromFile) + 1L;
                }
            } catch (final IOException e) {
                LOGGER.log(DEBUG, "Error reading latestAckBlockNumber file. Starting from block 0.");
            }
        }
        if (Files.exists(latestAckBlockHashPath)) {
            try {
                final byte[] fromFile = Files.readAllBytes(latestAckBlockHashPath);
                if (fromFile.length == StreamingTreeHasher.HASH_LENGTH) {
                    previousBlockHash = fromFile;
                }
            } catch (final IOException e) {
                LOGGER.log(DEBUG, "Error reading latestAckBlockHash file.");
            }
        }
    }

    /**
     * Returns the generation mode of this block stream manager.
     *
     * @return The GenerationMode enum value representing the current generation mode
     */
    @Override
    public GenerationMode getGenerationMode() {
        return generationMode;
    }

    /**
     * This operation is not supported in Craft mode.
     *
     * @throws UnsupportedOperationException always, as Craft mode does not support getting individual block items
     */
    @Override
    public BlockItem getNextBlockItem() {
        throw new UnsupportedOperationException("Craft mode does not support getting block items.");
    }

    /**
     * Generates and returns the next synthetic block.
     * Creates a block with a random number of events and transactions within configured bounds.
     * Each block includes a header, events with their transactions and results, and a proof.
     *
     * @return A newly generated Block
     * @throws IOException if there is an error processing block items
     * @throws BlockSimulatorParsingException if there is an error parsing block components
     */
    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException {
        LOGGER.log(DEBUG, "Started creation of block number %s.".formatted(currentBlockNumber));
        // todo(683) Refactor common hasher to accept protoc types, in order to avoid the additional overhead of keeping
        // and unparsing.
        final List<BlockItemUnparsed> blockItemsUnparsed = new ArrayList<>();
        final List<ItemHandler> items = new ArrayList<>();

        final ItemHandler headerItemHandler = new BlockHeaderHandler(previousBlockHash, currentBlockNumber);
        items.add(headerItemHandler);
        blockItemsUnparsed.add(headerItemHandler.unparseBlockItem());

        final int eventsNumber = random.nextInt(minEventsPerBlock, maxEventsPerBlock);
        for (int i = 0; i < eventsNumber; i++) {
            final ItemHandler eventHeaderHandler = new EventHeaderHandler();
            items.add(eventHeaderHandler);
            blockItemsUnparsed.add(eventHeaderHandler.unparseBlockItem());

            final int transactionsNumber = random.nextInt(minTransactionsPerEvent, maxTransactionsPerEvent);
            for (int j = 0; j < transactionsNumber; j++) {
                final ItemHandler eventTransactionHandler = new EventTransactionHandler();
                items.add(eventTransactionHandler);
                blockItemsUnparsed.add(eventTransactionHandler.unparseBlockItem());

                final ItemHandler transactionResultHandler = new TransactionResultHandler();
                items.add(transactionResultHandler);
                blockItemsUnparsed.add(transactionResultHandler.unparseBlockItem());
            }
        }

        LOGGER.log(DEBUG, "Appending %s number of block items in this block.".formatted(items.size()));

        processBlockItems(blockItemsUnparsed);
        updateCurrentBlockHash();

        ItemHandler proofItemHandler = new BlockProofHandler(previousBlockHash, currentBlockHash, currentBlockNumber);
        items.add(proofItemHandler);
        resetState();
        return Block.newBuilder()
                .addAllItems(items.stream().map(ItemHandler::getItem).toList())
                .build();
    }

    private void updateCurrentBlockHash() {
        com.hedera.hapi.block.stream.BlockProof unfinishedBlockProof =
                com.hedera.hapi.block.stream.BlockProof.newBuilder()
                        .previousBlockRootHash(Bytes.wrap(previousBlockHash))
                        .startOfBlockStateRootHash(Bytes.wrap(previousStateRootHash))
                        .build();

        currentBlockHash = HashingUtilities.computeFinalBlockHash(
                        unfinishedBlockProof, inputTreeHasher, outputTreeHasher)
                .toByteArray();
    }

    private void processBlockItems(List<BlockItemUnparsed> blockItems) {
        Hashes hashes = HashingUtilities.getBlockHashes(blockItems);
        while (hashes.inputHashes().hasRemaining()) {
            inputTreeHasher.addLeaf(hashes.inputHashes());
        }
        while (hashes.outputHashes().hasRemaining()) {
            outputTreeHasher.addLeaf(hashes.outputHashes());
        }
    }

    private void resetState() {
        inputTreeHasher = new NaiveStreamingTreeHasher();
        outputTreeHasher = new NaiveStreamingTreeHasher();
        currentBlockNumber++;
        previousBlockHash = currentBlockHash;
    }
}
