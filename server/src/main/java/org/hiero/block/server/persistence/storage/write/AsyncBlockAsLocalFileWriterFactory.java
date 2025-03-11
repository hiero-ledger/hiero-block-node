// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static java.lang.System.Logger.Level.TRACE;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import javax.inject.Inject;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.compression.Compression;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.persistence.storage.remove.BlockRemover;

/**
 * Factory for creating {@link AsyncBlockAsLocalFileWriter} instances.
 */
public final class AsyncBlockAsLocalFileWriterFactory implements AsyncBlockWriterFactory {
    private static final System.Logger LOGGER = System.getLogger(AsyncBlockAsLocalFileWriterFactory.class.getName());
    private final BlockPathResolver blockPathResolver;
    private final BlockRemover blockRemover;
    private final Compression compression;
    private final AckHandler ackHandler;
    private final MetricsService metricsService;

    @Inject
    public AsyncBlockAsLocalFileWriterFactory(
            @NonNull final BlockPathResolver blockPathResolver,
            @NonNull final BlockRemover blockRemover,
            @NonNull final Compression compression,
            @NonNull final AckHandler ackHandler,
            @NonNull final MetricsService metricsService) {
        this.blockPathResolver = Objects.requireNonNull(blockPathResolver);
        this.blockRemover = Objects.requireNonNull(blockRemover);
        this.compression = Objects.requireNonNull(compression);
        this.ackHandler = Objects.requireNonNull(ackHandler);
        this.metricsService = Objects.requireNonNull(metricsService);
    }

    @NonNull
    @Override
    public AsyncBlockWriter create(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        final AsyncBlockAsLocalFileWriter instance = new AsyncBlockAsLocalFileWriter(
                blockNumber, blockPathResolver, blockRemover, compression, ackHandler, metricsService);
        LOGGER.log(TRACE, "Created Writer for Block [%d]".formatted(blockNumber));
        return instance;
    }
}
