// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.BlocksPersisted;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.manager.BlockManager;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.compression.Compression;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A Block writer that handles writing of block-as-file.
 */
public final class BlockAsLocalFileWriter implements LocalBlockWriter<List<BlockItemUnparsed>> {
    private final MetricsService metricsService;
    private final BlockPathResolver blockPathResolver;
    private final Compression compression;
    private List<BlockItemUnparsed> currentBlockItems;
    private long currentBlockNumber = -1;
    private BlockManager blockManager;

    /**
     * Constructor.
     *
     * @param blockNodeContext valid, {@code non-null} instance of
     * {@link BlockNodeContext} used to get the {@link MetricsService}
     * @param blockPathResolver valid, {@code non-null} instance of
     * {@link BlockPathResolver} used to resolve paths to Blocks
     * @param compression valid, {@code non-null} instance of
     * {@link Compression} used to compress the Block
     */
    private BlockAsLocalFileWriter(
            @NonNull final BlockNodeContext blockNodeContext,
            @NonNull final BlockPathResolver blockPathResolver,
            @NonNull final Compression compression,
            @NonNull final BlockManager blockManager) {
        this.metricsService = Objects.requireNonNull(blockNodeContext.metricsService());
        this.blockPathResolver = Objects.requireNonNull(blockPathResolver);
        this.compression = Objects.requireNonNull(compression);
        this.blockManager = blockManager;
    }

    /**
     * This method creates and returns a new instance of {@link BlockAsLocalFileWriter}.
     *
     * @param blockNodeContext valid, {@code non-null} instance of
     * {@link BlockNodeContext} used to get the {@link MetricsService}
     * @param blockPathResolver valid, {@code non-null} instance of
     * {@link BlockPathResolver} used to resolve paths to Blocks
     * @param compression valid, {@code non-null} instance of
     * {@link Compression} used to compress the Block
     * @return a new, fully initialized instance of {@link BlockAsLocalFileWriter}
     */
    public static BlockAsLocalFileWriter of(
            @NonNull final BlockNodeContext blockNodeContext,
            @NonNull final BlockPathResolver blockPathResolver,
            @NonNull final Compression compression,
            @NonNull final BlockManager blockManager) {
        return new BlockAsLocalFileWriter(blockNodeContext, blockPathResolver, compression, blockManager);
    }

    @NonNull
    @Override
    public Optional<List<BlockItemUnparsed>> write(@NonNull final List<BlockItemUnparsed> valueToWrite)
            throws IOException, ParseException {
        final BlockItemUnparsed firstItem = valueToWrite.getFirst();
        if (firstItem.hasBlockHeader()) {
            currentBlockNumber = Preconditions.requireWhole(
                    BlockHeader.PROTOBUF.parse(firstItem.blockHeader()).number());
            currentBlockItems = new LinkedList<>(valueToWrite);
        } else {
            currentBlockItems.addAll(valueToWrite);
        }

        if (valueToWrite.getLast().hasBlockProof()) {
            final Optional<List<BlockItemUnparsed>> result = Optional.of(writeToFs());
            metricsService.get(BlocksPersisted).increment();
            blockManager.blockPersisted(currentBlockNumber);
            resetState();
            return result;
        } else {
            return Optional.empty();
        }
    }

    private List<BlockItemUnparsed> writeToFs() throws IOException {
        final Path rawBlockPath = blockPathResolver.resolveLiveRawPathToBlock(currentBlockNumber);
        final Path resolvedBlockPath =
                FileUtilities.appendExtension(rawBlockPath, compression.getCompressionFileExtension());
        FileUtilities.createFile(resolvedBlockPath);
        try (final OutputStream out = compression.wrap(Files.newOutputStream(resolvedBlockPath))) {
            final BlockUnparsed blockToWrite =
                    BlockUnparsed.newBuilder().blockItems(currentBlockItems).build();
            BlockUnparsed.PROTOBUF.toBytes(blockToWrite).writeTo(out);
        }
        return currentBlockItems;
    }

    private void resetState() {
        currentBlockItems = null;
        currentBlockNumber = -1;
    }
}
