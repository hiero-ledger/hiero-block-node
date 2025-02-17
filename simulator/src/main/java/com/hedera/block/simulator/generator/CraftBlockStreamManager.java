package com.hedera.block.simulator.generator;

import com.google.protobuf.ByteString;
import com.hedera.block.common.hasher.Hashes;
import com.hedera.block.common.hasher.HashingUtilities;
import com.hedera.block.common.hasher.NaiveStreamingTreeHasher;
import com.hedera.block.common.hasher.StreamingTreeHasher;
import com.hedera.block.simulator.config.data.BlockGeneratorConfig;
import com.hedera.block.simulator.config.types.GenerationMode;
import com.hedera.block.simulator.exception.BlockSimulatorParsingException;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hederahashgraph.api.proto.java.BlockHashAlgorithm;
import com.hederahashgraph.api.proto.java.SemanticVersion;
import com.hederahashgraph.api.proto.java.Timestamp;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

public class CraftBlockStreamManager implements BlockStreamManager{
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Configuration
    private final BlockGeneratorConfig blockGeneratorConfig;

    // State
    private final GenerationMode generationMode;
    private final byte[] previousStateRootHash;
    private byte[] previousBlockHash;
    private byte[] currentBlockHash;
    private long currentBlockNumber;
    private StreamingTreeHasher inputTreeHasher;
    private StreamingTreeHasher outputTreeHasher;

    public CraftBlockStreamManager(@NonNull BlockGeneratorConfig blockGeneratorConfig) {
        this.blockGeneratorConfig = requireNonNull(blockGeneratorConfig);
        this.generationMode = this.blockGeneratorConfig.generationMode();
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];

        this.previousBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.currentBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];

        this.currentBlockNumber = 0;
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();

        LOGGER.log(INFO, "Block Stream Simulator will use Craft mode for block management.");
    }

    @Override
    public GenerationMode getGenerationMode() {
        return generationMode;
    }

    @Override
    public BlockItem getNextBlockItem() {
        throw new UnsupportedOperationException("Craft mode does not support getting block items");
    }

    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException {
        LOGGER.log(DEBUG, "Started creation of block number %s.".formatted(currentBlockNumber));
        List<BlockItem> blockItems = new ArrayList<>();
        List<BlockItemUnparsed> blockItemsUnparsed = new ArrayList<>();

        BlockItem headerItem = BlockItem.newBuilder()
                .setBlockHeader(createBlockHeader())
                .build();
        blockItems.add(headerItem);

        // create all other block items with transactions and add them
        LOGGER.log(DEBUG, "Appending %s number of block items in this block.".formatted(currentBlockNumber));
        for (BlockItem blockItem : blockItems) {
            BlockItemUnparsed blockItemUnparsed = unparseBlockItem(blockItem);
            blockItemsUnparsed.add(blockItemUnparsed);
        }
        processBlockItems(blockItemsUnparsed);
        updateCurrentBlockHash();

        // create random number of block items with random type of transactions depending on type from configuration
        BlockItem proofItem = BlockItem.newBuilder()
                .setBlockProof(createBlockProof())
                .build();
        blockItems.add(proofItem);
        resetState();
        return Block.newBuilder().addAllItems(blockItems).build();
    }

    private void updateCurrentBlockHash() {
        com.hedera.hapi.block.stream.BlockProof unfinishedBlockProof = com.hedera.hapi.block.stream.BlockProof.newBuilder()
                .previousBlockRootHash(Bytes.wrap(previousBlockHash))
                .startOfBlockStateRootHash(Bytes.wrap(previousStateRootHash))
                .build();

        currentBlockHash = HashingUtilities.computeFinalBlockHash(unfinishedBlockProof, inputTreeHasher, outputTreeHasher).toByteArray();
    }

    private BlockItemUnparsed unparseBlockItem(BlockItem blockItem) throws BlockSimulatorParsingException {
        try {
            return BlockItemUnparsed.PROTOBUF.parse(Bytes.wrap(blockItem.toByteArray()));
        } catch (ParseException e) {
            throw new BlockSimulatorParsingException(e);
        }
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

    private BlockHeader createBlockHeader() {
        final SemanticVersion semanticVersion = SemanticVersion.newBuilder().setMajor(0).setMinor(1).setPatch(0).build();
        final Timestamp firstTransactionConsensusTime = Timestamp.newBuilder().setSeconds(System.currentTimeMillis()/1000).build();
        return BlockHeader.newBuilder()
                .setHapiProtoVersion(semanticVersion)
                .setSoftwareVersion(semanticVersion)
                .setHashAlgorithm(BlockHashAlgorithm.SHA2_384)
                .setFirstTransactionConsensusTime(firstTransactionConsensusTime)
                .setPreviousBlockHash(ByteString.copyFrom(previousBlockHash))
                .setNumber(currentBlockNumber)
                .build();
    }

    private BlockProof createBlockProof() {
        return BlockProof.newBuilder()
                .setBlock(currentBlockNumber)
                .setPreviousBlockRootHash(ByteString.copyFrom(previousBlockHash))
                .setStartOfBlockStateRootHash(ByteString.copyFrom(previousStateRootHash))
                .setBlockSignature(produceSignature())
                .build();
    }

    private ByteString produceSignature() {
        return ByteString.copyFrom(HashingUtilities.noThrowSha384HashOf(currentBlockHash));
    }
}
