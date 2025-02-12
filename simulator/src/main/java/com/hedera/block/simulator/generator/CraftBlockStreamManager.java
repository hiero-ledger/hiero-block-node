package com.hedera.block.simulator.generator;

import com.google.protobuf.ByteString;
import com.hedera.block.common.hasher.StreamingTreeHasher;
import com.hedera.block.simulator.config.data.BlockGeneratorConfig;
import com.hedera.block.simulator.config.types.GenerationMode;
import com.hedera.block.simulator.exception.BlockSimulatorParsingException;
import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
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
    private final ByteString blockSignature;
    private final ByteString previousStateRootHash;
    private ByteString previousBlockHash;
    private long currentBlockNumber;

    public CraftBlockStreamManager(@NonNull BlockGeneratorConfig blockGeneratorConfig) {
        this.blockGeneratorConfig = requireNonNull(blockGeneratorConfig);
        this.generationMode = this.blockGeneratorConfig.generationMode();
        this.currentBlockNumber = 0;
        this.blockSignature = ByteString.copyFrom(new byte[StreamingTreeHasher.HASH_LENGTH]);
        this.previousBlockHash = ByteString.copyFrom(new byte[StreamingTreeHasher.HASH_LENGTH]);
        this.previousStateRootHash = ByteString.copyFrom(new byte[StreamingTreeHasher.HASH_LENGTH]);

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
        LOGGER.log(DEBUG, "Started creation of block number %s.".formatted(this.currentBlockNumber));
        List<BlockItem> blockItems = new ArrayList<>();
        BlockItem headerItem = BlockItem.newBuilder()
                .setBlockHeader(createBlockHeader())
                .build();
        blockItems.add(headerItem);

        // create random number of block items with random type of transactions depending on type from configuration
        LOGGER.log(DEBUG, "Appending %s number of block items in this block.".formatted(this.currentBlockNumber));
        BlockItem proofItem = BlockItem.newBuilder()
                .setBlockProof(createBlockProof())
                .build();
        blockItems.add(proofItem);

        // depending on the block items calculate new current block hash
        // depending on the block items those calculate new state root hash
        incrementBlock();
        return Block.newBuilder().addAllItems(blockItems).build();
    }

    private void incrementBlock() {
        this.currentBlockNumber++;
    }

    private BlockHeader createBlockHeader() {
        final SemanticVersion semanticVersion = SemanticVersion.newBuilder().setMajor(0).setMinor(1).setPatch(0).build();
        final Timestamp firstTransactionConsensusTime = Timestamp.newBuilder().setSeconds(System.currentTimeMillis()/1000).build();
        return BlockHeader.newBuilder()
                .setHapiProtoVersion(semanticVersion)
                .setSoftwareVersion(semanticVersion)
                .setHashAlgorithm(BlockHashAlgorithm.SHA2_384)
                .setFirstTransactionConsensusTime(firstTransactionConsensusTime)
                .setPreviousBlockHash(this.previousBlockHash)
                .setNumber(this.currentBlockNumber).build();
    }

    private BlockProof createBlockProof() {
        return BlockProof.newBuilder()
                .setBlock(this.currentBlockNumber)
                .setPreviousBlockRootHash(this.previousBlockHash)
                .setStartOfBlockStateRootHash(this.previousStateRootHash)
                .setBlockSignature(this.blockSignature)
                .build();
    }
}
