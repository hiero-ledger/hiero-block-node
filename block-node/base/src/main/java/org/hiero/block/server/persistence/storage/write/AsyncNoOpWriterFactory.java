// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static java.lang.System.Logger.Level.TRACE;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;

/**
 * A factory that creates {@link AsyncNoOpWriter} instances.
 */
public final class AsyncNoOpWriterFactory implements AsyncBlockWriterFactory {
    private static final System.Logger LOGGER = System.getLogger(AsyncNoOpWriterFactory.class.getName());
    private final AckHandler ackHandler;
    private final MetricsService metricsService;

    public AsyncNoOpWriterFactory(@NonNull final AckHandler ackHandler, @NonNull final MetricsService metricsService) {
        this.ackHandler = Objects.requireNonNull(ackHandler);
        this.metricsService = Objects.requireNonNull(metricsService);
    }

    /**
     * Creates a new {@link AsyncNoOpWriter} instance.
     *
     * @param blockNumber the block number for the block that this writer will
     * process, no preconditions check for the block number
     * @return a new {@link AsyncNoOpWriter} instance
     */
    @NonNull
    @Override
    public AsyncBlockWriter create(long blockNumber) {
        final AsyncNoOpWriter instance = new AsyncNoOpWriter(blockNumber, ackHandler, metricsService);
        LOGGER.log(TRACE, "Created Writer for Block [%d]".formatted(blockNumber));
        return instance;
    }
}
