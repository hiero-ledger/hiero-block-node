// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands;

import com.hedera.hapi.block.stream.output.protoc.BlockFooter;
import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.hiero.block.common.hasher.Hashes;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.generator.itemhandler.BlockFooterHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.BlockProofHandler;
import org.hiero.block.simulator.generator.itemhandler.EventHeaderHandler;
import org.hiero.block.simulator.generator.itemhandler.ItemHandler;
import org.hiero.block.simulator.generator.itemhandler.SignedTransactionHandler;
import org.hiero.block.simulator.generator.itemhandler.TransactionResultHandler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI tool to quickly generate blocks for testing purposes.
 */
@Command(name = "generate", description = "CLI tool to quickly generate blocks for testing purposes.")
public class BlockGenerator implements Runnable {

    @Option(
            names = {"-o", "--out"},
            description = "Output path for generated blocks",
            required = true)
    private Path out;

    @Option(
            names = {"-c", "--count"},
            description = "Start of block range (inclusive)",
            required = true)
    private long blockGenerationCount;

    private final Random random = new Random();
    private long currentBlockNumber;
    private byte[] previousBlockHash;
    private byte[] currentBlockHash;
    private BlockHeader currentBlockHeader;
    private BlockFooter currentBlockFooter;
    private int minEventsPerBlock = 1;
    private int maxEventsPerBlock = 10;
    private int minTransactionsPerEvent = 1;
    private int maxTransactionsPerEvent = 10;
    private StreamingTreeHasher inputTreeHasher;
    private StreamingTreeHasher outputTreeHasher;
    private StreamingTreeHasher consensusHeaderHasher;
    private StreamingTreeHasher stateChangesHasher;
    private StreamingTreeHasher traceDataHasher;

    @Override
    public void run() {
        out = out.toAbsolutePath();
        System.out.printf("Generating %d blocks to %s%n", blockGenerationCount, out);
        // Implementation for block generation would go here
        try {
            for (int i = 0; i < blockGenerationCount; i++) {
                final Block block = createNextBlock();
            }
        } catch (final BlockSimulatorParsingException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private Block createNextBlock() throws BlockSimulatorParsingException, ParseException {
        System.out.printf("Started creation of block number %d%n", currentBlockNumber);
        // todo(683) Refactor common hasher to accept protoc types, in order to avoid the additional overhead of keeping
        // and unparsing.
        final List<BlockItemUnparsed> blockItemsUnparsed = new ArrayList<>();
        final List<ItemHandler> items = new ArrayList<>();

        final ItemHandler headerItemHandler = new BlockHeaderHandler(previousBlockHash, currentBlockNumber);
        items.add(headerItemHandler);
        blockItemsUnparsed.add(headerItemHandler.unparseBlockItem());
        currentBlockHeader = headerItemHandler.getItem().getBlockHeader();

        final int eventsNumber = random.nextInt(minEventsPerBlock, maxEventsPerBlock);
        for (int i = 0; i < eventsNumber; i++) {
            final ItemHandler eventHeaderHandler = new EventHeaderHandler();
            items.add(eventHeaderHandler);
            blockItemsUnparsed.add(eventHeaderHandler.unparseBlockItem());

            final int transactionsNumber = random.nextInt(minTransactionsPerEvent, maxTransactionsPerEvent);
            for (int j = 0; j < transactionsNumber; j++) {
                final ItemHandler eventTransactionHandler = new SignedTransactionHandler();
                items.add(eventTransactionHandler);
                blockItemsUnparsed.add(eventTransactionHandler.unparseBlockItem());
                final ItemHandler transactionResultHandler = new TransactionResultHandler(0, 0);
                items.add(transactionResultHandler);
                blockItemsUnparsed.add(transactionResultHandler.unparseBlockItem());
            }
        }

        System.out.printf("Appending %d number of block items in this block.%n", items.size());

        processBlockItems(blockItemsUnparsed);

        ItemHandler footerItemHandler = new BlockFooterHandler(previousBlockHash);
        items.add(footerItemHandler);
        currentBlockFooter = footerItemHandler.getItem().getBlockFooter();

        updateCurrentBlockHash();

        final ItemHandler proofItemHandler = new BlockProofHandler(currentBlockHash, currentBlockNumber);
        items.add(proofItemHandler);
        resetState();
        return Block.newBuilder()
                .addAllItems(items.stream().map(ItemHandler::getItem).toList())
                .build();
    }

    private void processBlockItems(List<BlockItemUnparsed> blockItems) {
        final Hashes hashes = HashingUtilities.getBlockHashes(blockItems);
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

    private void updateCurrentBlockHash() throws ParseException {
        com.hedera.hapi.block.stream.output.BlockHeader blockHeader =
                com.hedera.hapi.block.stream.output.BlockHeader.PROTOBUF.parse(
                        Bytes.wrap(currentBlockHeader.toByteArray()));
        com.hedera.hapi.block.stream.output.BlockFooter blockFooter =
                com.hedera.hapi.block.stream.output.BlockFooter.PROTOBUF.parse(
                        Bytes.wrap(currentBlockFooter.toByteArray()));

        currentBlockHash = HashingUtilities.computeFinalBlockHash(
                        blockHeader,
                        blockFooter,
                        inputTreeHasher,
                        outputTreeHasher,
                        consensusHeaderHasher,
                        stateChangesHasher,
                        traceDataHasher,
                        Bytes.EMPTY)
                .toByteArray();
    }
}
