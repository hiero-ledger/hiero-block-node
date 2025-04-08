// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.hiero.block.common.hasher.Hashes;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.itemhandler.BlockHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockProofHandler;
import org.hiero.block.simulator.generator.itemhandler.EventHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.EventTransactionHandler;
import org.hiero.block.simulator.generator.itemhandler.ItemHandler;
import org.hiero.block.simulator.generator.itemhandler.TransactionResultHandler;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * Implementation of BlockStreamManager that crafts blocks from scratch rather than reading from an existing stream.
 * This manager generates synthetic blocks with random events and transactions based on configured parameters.
 */
public class CraftBlockStreamManager implements BlockStreamManager {
    private final Logger LOGGER = System.getLogger(getClass().getName());

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
    private Block block;

    /**
     * Constructs a new CraftBlockStreamManager with the specified configuration.
     *
     * @param blockGeneratorConfig Configuration parameters for block generation
     * including event and transaction counts
     * @param simulatorStartupData simulator startup data used for initialization
     * @throws NullPointerException if blockGeneratorConfig is null
     */
    public CraftBlockStreamManager(
            @NonNull final BlockGeneratorConfig blockGeneratorConfig,
            @NonNull final SimulatorStartupData simulatorStartupData) {
        this.generationMode = blockGeneratorConfig.generationMode();
        this.minEventsPerBlock = blockGeneratorConfig.minEventsPerBlock();
        this.maxEventsPerBlock = blockGeneratorConfig.maxEventsPerBlock();
        this.minTransactionsPerEvent = blockGeneratorConfig.minTransactionsPerEvent();
        this.maxTransactionsPerEvent = blockGeneratorConfig.maxTransactionsPerEvent();
        this.random = new Random();
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
        this.currentBlockNumber = simulatorStartupData.getLatestAckBlockNumber() + 1L;
        this.previousBlockHash = simulatorStartupData.getLatestAckBlockHash();
        LOGGER.log(INFO, "Block Stream Simulator will use Craft mode for block management");
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
        block = Block.newBuilder()
                .addAllItems(items.stream().map(ItemHandler::getItem).toList())
                .build();
        return block;
    }

    @Override
    public Block getLastBlock() throws IOException, BlockSimulatorParsingException {
        if (block == null) {
            throw new IllegalStateException("No block has been generated yet.");
        }
        return block;
    }

    @Override
    public Block getBlockByNumber(long blockNumber) throws IOException, BlockSimulatorParsingException {
        throw new UnsupportedOperationException("Craft mode does not support fetching specific blocks by number.");
    }

    private void updateCurrentBlockHash() {
        BlockProof unfinishedBlockProof = BlockProof.newBuilder()
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
