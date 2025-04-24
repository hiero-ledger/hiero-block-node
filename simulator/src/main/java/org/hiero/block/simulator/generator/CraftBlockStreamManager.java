// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.block.common.hasher.Hashes;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.itemhandler.BlockHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockProofHandler;
import org.hiero.block.simulator.generator.itemhandler.EventHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.EventTransactionHandler;
import org.hiero.block.simulator.generator.itemhandler.ItemHandler;
import org.hiero.block.simulator.generator.itemhandler.TransactionResultHandler;
import org.hiero.block.simulator.startup.SimulatorStartupData;

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
    private final boolean invalidBlockHash;

    // State
    private final GenerationMode generationMode;
    private final byte[] previousStateRootHash;
    private byte[] previousBlockHash;
    private byte[] currentBlockHash;
    private long currentBlockNumber;
    private StreamingTreeHasher inputTreeHasher;
    private StreamingTreeHasher outputTreeHasher;

    // Unordered streaming
    private final boolean unorderedStreamingEnabled;
    private Iterator<Block> unorderedStreamIterator;

    /**
     * Constructs a new CraftBlockStreamManager with the specified configuration.
     *
     * @param blockGeneratorConfig Configuration parameters for block generation
     *                             including event and transaction counts
     * @param simulatorStartupData simulator startup data used for initialization
     * @throws NullPointerException if blockGeneratorConfig is null
     */
    public CraftBlockStreamManager(
            @NonNull final BlockGeneratorConfig blockGeneratorConfig,
            @NonNull final SimulatorStartupData simulatorStartupData,
            @NonNull final UnorderedStreamConfig unorderedStreamConfig) {
        this.generationMode = blockGeneratorConfig.generationMode();
        this.minEventsPerBlock = blockGeneratorConfig.minEventsPerBlock();
        this.maxEventsPerBlock = blockGeneratorConfig.maxEventsPerBlock();
        this.minTransactionsPerEvent = blockGeneratorConfig.minTransactionsPerEvent();
        this.maxTransactionsPerEvent = blockGeneratorConfig.maxTransactionsPerEvent();
        this.invalidBlockHash = blockGeneratorConfig.invalidBlockHash();
        this.random = new Random();
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
        this.currentBlockNumber = simulatorStartupData.getLatestAckBlockNumber() + 1L;
        this.previousBlockHash = simulatorStartupData.getLatestAckBlockHash();
        LOGGER.log(INFO, "Block Stream Simulator will use Craft mode for block management");

        // Unordered streaming
        unorderedStreamingEnabled = unorderedStreamConfig.enabled();
        if (unorderedStreamingEnabled) {
            initUnorderedStreaming(simulatorStartupData, unorderedStreamConfig);
        }
    }

    private void initUnorderedStreaming(
            SimulatorStartupData simulatorStartupData, UnorderedStreamConfig unorderedStreamConfig) {

        if (simulatorStartupData.isEnabled()) {
            throw new IllegalStateException("Unordered streaming does not support start-up data enabled");
        }
        this.currentBlockNumber = 0;
        final List<Block> blockStreamList;
        final int scrambleLevel = unorderedStreamConfig.sequenceScrambleLevel();
        if (scrambleLevel == 0) {
            // use a predefined order of streaming from the properties file
            final LinkedHashSet<Long> fixedStreamingSequence =
                    parseRangeSet(unorderedStreamConfig.fixedStreamingSequence());
            blockStreamList = getBlockStreamList(fixedStreamingSequence);
        } else {
            // put the predefined available blocks in a list and scramble by the provided coefficient
            final Set<Long> availableBlocks = parseRangeSet(unorderedStreamConfig.availableBlocks());
            blockStreamList = getBlockStreamList(availableBlocks);
            scrambleBlocks(blockStreamList, scrambleLevel);
        }
        if (blockStreamList.isEmpty()) {
            throw new IllegalStateException("No blocks are available for streaming with the current configuration");
        }
        unorderedStreamIterator = blockStreamList.iterator();
        LOGGER.log(INFO, "Unordered streaming is enabled");
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
     * @throws IOException                    if there is an error processing block items
     * @throws BlockSimulatorParsingException if there is an error parsing block components
     */
    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException {
        if (unorderedStreamingEnabled) {
            if (unorderedStreamIterator.hasNext()) {
                return unorderedStreamIterator.next();
            }
            return null;
        } else {
            return createNextBlock();
        }
    }

    private Block createNextBlock() throws BlockSimulatorParsingException {
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

        // if the block hash is invalid, generate a random hash and overwrite the legitimate one
        if (invalidBlockHash) {
            currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
            random.nextBytes(currentBlockHash);
        }

        ItemHandler proofItemHandler = new BlockProofHandler(previousBlockHash, currentBlockHash, currentBlockNumber);
        items.add(proofItemHandler);
        resetState();
        return Block.newBuilder()
                .addAllItems(items.stream().map(ItemHandler::getItem).toList())
                .build();
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

    private List<Block> getBlockStreamList(Set<Long> blockSequence) {
        requireNonNull(blockSequence);
        final Map<Long, Block> craftedBlocksMap = new HashMap<>();
        final List<Block> blockStreamList = new ArrayList<>();
        if (!blockSequence.isEmpty()) {
            try {
                for (int i = 0; i <= Collections.max(blockSequence); i++) {
                    final Block block = createNextBlock();
                    final long blockNbr = block.getItems(block.getItemsCount() - 1)
                            .getBlockProof()
                            .getBlock();
                    craftedBlocksMap.put(blockNbr, block);
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, e.getMessage(), e);
                throw new RuntimeException(e);
            }
            for (Long blockNbr : blockSequence) {
                if (craftedBlocksMap.containsKey(blockNbr)) {
                    blockStreamList.add(craftedBlocksMap.get(blockNbr));
                }
            }
        }
        return blockStreamList;
    }

    private void scrambleBlocks(List<Block> list, int coefficient) {
        requireNonNull(list);
        final int size = list.size();
        if (size > 1) {
            final List<Block> originalList = new ArrayList<>(list);
            final Random random = new Random();
            final int maxAttempts = 10;
            int attempt = 0;

            // The number of swaps is proportional to the coefficient
            final int swapCount = (int) ((coefficient / 10.0) * (size * 2));

            while (list.equals(originalList) && attempt++ < maxAttempts) {
                for (int i = 0; i < swapCount; i++) {
                    final int index1 = random.nextInt(size - 1);
                    final int index2;
                    if (coefficient <= 5) {
                        // When coefficient is low, swap with a neighbor
                        index2 = index1 + 1;
                    } else {
                        // Higher coefficient allows more distant swaps
                        index2 = random.nextInt(size);
                    }
                    Collections.swap(list, index1, index2);
                }
            }

            // this condition is not supposed to be met when valid configurations are provided
            // maxAttempts variable serves for endless loop prevention in case of inaccurate config inputs
            if (list.equals(originalList)) {
                LOGGER.log(WARNING, "Scramble unsuccessful after " + maxAttempts + " attempts");
            }
        }
    }

    private LinkedHashSet<Long> parseRangeSet(String input) {
        requireNonNull(input);
        final LinkedHashSet<Long> result = new LinkedHashSet<>();
        if (!input.isBlank()) {
            try {
                final Matcher matcher =
                        Pattern.compile("\\[(\\d+)-(\\d+)\\]|(\\d+)").matcher(input);
                while (matcher.find()) {
                    if (matcher.group(1) != null && matcher.group(2) != null) {
                        final long start = Long.parseLong(matcher.group(1));
                        final long end = Long.parseLong(matcher.group(2));
                        if (start <= 0 || end <= 0) {
                            throw new IllegalArgumentException(input
                                    + " does not match the expected format. Range values must be positive and non-zero: ["
                                    + start + "-" + end + "]");
                        }
                        if (start >= end) {
                            throw new IllegalArgumentException(
                                    input + " does not match the expected format. Invalid range: start >= end in ["
                                            + start + "-" + end + "]");
                        }
                        for (long i = start; i <= end; i++) {
                            result.add(i);
                        }
                    } else if (matcher.group(3) != null) {
                        final long value = Long.parseLong(matcher.group(3));
                        if (value <= 0) {
                            throw new IllegalArgumentException("Values must be positive and non-zero: " + value);
                        }
                        result.add(value);
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Exception in parsing input: " + input, e);
            }
        }
        return result;
    }
}
