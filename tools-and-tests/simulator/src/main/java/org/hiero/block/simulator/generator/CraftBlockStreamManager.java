// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.output.protoc.BlockFooter;
import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger;
import java.security.NoSuchAlgorithmException;
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
import org.hiero.block.common.hasher.StreamingHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.itemhandler.BlockFooterHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockProofHandler;
import org.hiero.block.simulator.generator.itemhandler.EventHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.ItemHandler;
import org.hiero.block.simulator.generator.itemhandler.SignedTransactionHandler;
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
    private final int endBlockNumber;

    // State
    private final GenerationMode generationMode;
    private final byte[] previousStateRootHash;
    private byte[] previousBlockHash;
    private byte[] currentBlockHash;
    private long currentBlockNumber;
    private StreamingTreeHasher inputTreeHasher;
    private StreamingTreeHasher outputTreeHasher;
    private StreamingTreeHasher consensusHeaderHasher;
    private StreamingTreeHasher stateChangesHasher;
    private StreamingTreeHasher traceDataHasher;
    private BlockHeader currentBlockHeader;
    private BlockFooter currentBlockFooter;

    // Unordered streaming
    private final boolean unorderedStreamingEnabled;
    private Iterator<Block> unorderedStreamIterator;

    private final BlockGeneratorConfig blockGeneratorConfig;
    private final SimulatorStartupData simulatorStartupData;

    StreamingHasher rootHashOfAllBlockHashesTreeHasher;
    Bytes ZERO_BLOCK_HASH = Bytes.wrap(new byte[48]);

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
        this.blockGeneratorConfig = blockGeneratorConfig;
        this.generationMode = blockGeneratorConfig.generationMode();
        this.minEventsPerBlock = blockGeneratorConfig.minEventsPerBlock();
        this.maxEventsPerBlock = blockGeneratorConfig.maxEventsPerBlock();
        this.minTransactionsPerEvent = blockGeneratorConfig.minTransactionsPerEvent();
        this.maxTransactionsPerEvent = blockGeneratorConfig.maxTransactionsPerEvent();
        this.invalidBlockHash = blockGeneratorConfig.invalidBlockHash();
        this.endBlockNumber = blockGeneratorConfig.endBlockNumber();
        this.random = new Random();
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
        this.consensusHeaderHasher = new NaiveStreamingTreeHasher();
        this.stateChangesHasher = new NaiveStreamingTreeHasher();
        this.traceDataHasher = new NaiveStreamingTreeHasher();
        this.simulatorStartupData = simulatorStartupData;
        // currently we are not supporting startup saved data due to the calculation of the
        // root hash of all block hashes tree hasher
        this.currentBlockNumber = 0;
        this.previousBlockHash = ZERO_BLOCK_HASH.toByteArray();
        LOGGER.log(INFO, "Block Stream Simulator will use Craft mode for block management");

        // Unordered streaming
        unorderedStreamingEnabled = unorderedStreamConfig.enabled();
        if (unorderedStreamingEnabled) {
            initUnorderedStreaming(simulatorStartupData, unorderedStreamConfig);
        }

        // init root hash of all block hashes tree hasher
        initRootHashOfAllBlockHashesTreeHasher();

        if (simulatorStartupData.getLatestAckBlockNumber() >= 0) {
            final long targetBlock = simulatorStartupData.getLatestAckBlockNumber() + 1L;
            resetToBlock(targetBlock);
        }
    }

    /***
     * Initializes the hasher for the root hash of all block hashes tree.
     * currently only supports from genesis, in the future we might consider
     * initializing from saved state on file.
     */
    private void initRootHashOfAllBlockHashesTreeHasher() {
        try {
            this.rootHashOfAllBlockHashesTreeHasher = new StreamingHasher();
            this.rootHashOfAllBlockHashesTreeHasher.addLeaf(ZERO_BLOCK_HASH.toByteArray());
        } catch (NoSuchAlgorithmException e) {
            LOGGER.log(ERROR, "Error initializing rootHashOfAllBlockHashesTreeHasher", e);
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
     * Generates and returns the next synthetic block.
     * Creates a block with a random number of events and transactions within configured bounds.
     * Each block includes a header, events with their transactions and results, and a proof.
     *
     * @return A newly generated Block
     * @throws IOException                    if there is an error processing block items
     * @throws BlockSimulatorParsingException if there is an error parsing block components
     */
    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException, ParseException {
        if (endBlockNumber > 0 && currentBlockNumber > endBlockNumber) {
            return null;
        }

        if (unorderedStreamingEnabled) {
            if (unorderedStreamIterator.hasNext()) {
                return unorderedStreamIterator.next();
            }
            return null;
        } else {
            return createNextBlock();
        }
    }

    @Override
    public void resetToBlock(final long block) {
        try {
            LOGGER.log(DEBUG, "Resetting to block number {0}. current block number = {1}", block, currentBlockNumber);
            // if current block is bigger, we reset state and start from 0 up to block
            if (currentBlockNumber > block) {
                LOGGER.log(
                        DEBUG,
                        "Current block number {0} is greater than target block number {1}. Resetting state and starting from block 0.",
                        currentBlockNumber,
                        block);
                resetState();
                currentBlockNumber = 0;
                previousBlockHash = ZERO_BLOCK_HASH.toByteArray();
                initRootHashOfAllBlockHashesTreeHasher();
            }

            while (currentBlockNumber < block) {
                createNextBlock();
            }
            LOGGER.log(
                    DEBUG,
                    "Reset to block number {0} completed. current block number = {1}",
                    block,
                    currentBlockNumber);
        } catch (BlockSimulatorParsingException | ParseException e) {
            LOGGER.log(ERROR, "Error while resetting to block " + block, e);
            throw new RuntimeException(e);
        }
    }

    private Block createNextBlock() throws BlockSimulatorParsingException, ParseException {
        LOGGER.log(DEBUG, "Started creation of block number {0}.", currentBlockNumber);
        // @todo(#683) Refactor common hasher to accept protoc types, in order to avoid the additional overhead of
        // keeping and unparsing.
        final List<BlockItemUnparsed> blockItemsUnparsed = new ArrayList<>();
        final List<ItemHandler> items = new ArrayList<>();

        final ItemHandler headerItemHandler = new BlockHeaderHandler(previousBlockHash, currentBlockNumber);
        items.add(headerItemHandler);
        blockItemsUnparsed.add(headerItemHandler.unparseBlockItem());
        currentBlockHeader = headerItemHandler.getItem().getBlockHeader();
        final int eventsNumber = minEventsPerBlock; // for deterministic testing
        for (int i = 0; i < eventsNumber; i++) {
            final ItemHandler eventHeaderHandler = new EventHeaderHandler();
            items.add(eventHeaderHandler);
            blockItemsUnparsed.add(eventHeaderHandler.unparseBlockItem());
            final int transactionsNumber = minTransactionsPerEvent; // for deterministic testing
            for (int j = 0; j < transactionsNumber; j++) {
                final ItemHandler eventTransactionHandler = new SignedTransactionHandler();
                items.add(eventTransactionHandler);
                blockItemsUnparsed.add(eventTransactionHandler.unparseBlockItem());

                final ItemHandler transactionResultHandler = new TransactionResultHandler(blockGeneratorConfig);
                items.add(transactionResultHandler);
                blockItemsUnparsed.add(transactionResultHandler.unparseBlockItem());
            }
        }

        LOGGER.log(DEBUG, "Appending {0} number of block items in this block.", items.size());

        processBlockItems(blockItemsUnparsed);

        byte[] rootHashOfAllBlockHashesTree = null;
        if (rootHashOfAllBlockHashesTreeHasher != null) {
            rootHashOfAllBlockHashesTree = rootHashOfAllBlockHashesTreeHasher.computeRootHash();
        }
        ItemHandler footerItemHandler = new BlockFooterHandler(previousBlockHash, rootHashOfAllBlockHashesTree);
        items.add(footerItemHandler);
        currentBlockFooter = footerItemHandler.getItem().getBlockFooter();

        updateCurrentBlockHash();
        simulatorStartupData.addBlockHash(currentBlockNumber, currentBlockHash);

        // if the block hash is invalid, generate a random hash and overwrite the legitimate one
        if (invalidBlockHash) {
            currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
            random.nextBytes(currentBlockHash);
        }

        ItemHandler proofItemHandler = new BlockProofHandler(currentBlockHash, currentBlockNumber);
        items.add(proofItemHandler);
        LOGGER.log(DEBUG, "Created block number {0} with hash {1}", currentBlockNumber, Bytes.wrap(currentBlockHash));

        if (rootHashOfAllBlockHashesTreeHasher != null) {
            rootHashOfAllBlockHashesTreeHasher.addLeaf(currentBlockHash);
        }

        resetState();

        return Block.newBuilder()
                .addAllItems(items.stream().map(ItemHandler::getItem).toList())
                .build();
    }

    private void updateCurrentBlockHash() throws ParseException {
        com.hedera.hapi.block.stream.output.BlockHeader blockHeader =
                com.hedera.hapi.block.stream.output.BlockHeader.PROTOBUF.parse(
                        Bytes.wrap(currentBlockHeader.toByteArray()));
        com.hedera.hapi.block.stream.output.BlockFooter blockFooter =
                com.hedera.hapi.block.stream.output.BlockFooter.PROTOBUF.parse(
                        Bytes.wrap(currentBlockFooter.toByteArray()));

        currentBlockHash = HashingUtilities.computeFinalBlockHash(
                        blockHeader.blockTimestamp(),
                        blockFooter.previousBlockRootHash(),
                        blockFooter.rootHashOfAllBlockHashesTree(),
                        blockFooter.startOfBlockStateRootHash(),
                        inputTreeHasher,
                        outputTreeHasher,
                        consensusHeaderHasher,
                        stateChangesHasher,
                        traceDataHasher)
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
        while (hashes.consensusHeaderHashes().hasRemaining()) {
            consensusHeaderHasher.addLeaf(hashes.consensusHeaderHashes());
        }
        while (hashes.stateChangesHashes().hasRemaining()) {
            stateChangesHasher.addLeaf(hashes.stateChangesHashes());
        }
        while (hashes.traceDataHashes().hasRemaining()) {
            traceDataHasher.addLeaf(hashes.traceDataHashes());
        }
    }

    private void resetState() {
        // Reset the hasher states for the next block
        inputTreeHasher = new NaiveStreamingTreeHasher();
        outputTreeHasher = new NaiveStreamingTreeHasher();
        consensusHeaderHasher = new NaiveStreamingTreeHasher();
        stateChangesHasher = new NaiveStreamingTreeHasher();
        traceDataHasher = new NaiveStreamingTreeHasher();
        // Reset the previous root hash to the current block hash
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
                LOGGER.log(WARNING, "Scramble unsuccessful after {0} attempts", maxAttempts);
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
