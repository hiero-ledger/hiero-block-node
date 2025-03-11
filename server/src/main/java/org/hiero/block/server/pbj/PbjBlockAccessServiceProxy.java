// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.SingleBlocksNotFound;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.SingleBlocksRetrieved;

import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SingleBlockRequest;
import com.hedera.hapi.block.SingleBlockResponseCode;
import com.hedera.hapi.block.SingleBlockResponseUnparsed;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Optional;
import javax.inject.Inject;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.service.ServiceStatus;

/**
 * PbjBlockAccessServiceProxy is the runtime binding between the PBJ Helidon Plugin and the
 * Block Node. The Helidon Plugin routes inbound requests to this class based on the methods
 * and service names in PbjBlockAccessService. Service implementations are instantiated via
 * the open method thereby bridging the client requests into the Block Node application.
 */
public class PbjBlockAccessServiceProxy implements PbjBlockAccessService {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final ServiceStatus serviceStatus;
    private final BlockReader<BlockUnparsed> blockReader;
    private final MetricsService metricsService;

    /**
     * Creates a new PbjBlockAccessServiceProxy instance.
     *
     * @param serviceStatus the service status
     * @param blockReader the block reader
     * @param metricsService the metrics service
     */
    @Inject
    public PbjBlockAccessServiceProxy(
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final MetricsService metricsService) {
        this.serviceStatus = serviceStatus;
        this.blockReader = blockReader;
        this.metricsService = metricsService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Pipeline<? super Bytes> open(
            final @NonNull Method method,
            final @NonNull RequestOptions options,
            final @NonNull Pipeline<? super Bytes> replies) {

        try {
            final var m = (BlockAccessMethod) method;
            return switch (m) {
                case singleBlock -> Pipelines.<SingleBlockRequest, SingleBlockResponseUnparsed>unary()
                        .mapRequest(bytes -> parseSingleBlockRequest(bytes))
                        .method(this::singleBlock)
                        .mapResponse(reply -> createSingleBlockResponse(reply, options))
                        .respondTo(replies)
                        .build();
            };
        } catch (Exception e) {
            replies.onError(e);
            return Pipelines.noop();
        }
    }

    /**
     * Executes the unary singleBlock gRPC method.
     *
     * @param singleBlockRequest the single block request
     * @return the single block response
     */
    SingleBlockResponseUnparsed singleBlock(SingleBlockRequest singleBlockRequest) {

        LOGGER.log(DEBUG, "Executing Unary singleBlock gRPC method");

        if (serviceStatus.isRunning()) {
            final long blockNumber = singleBlockRequest.blockNumber();
            try {
                final Optional<BlockUnparsed> blockOpt = blockReader.read(blockNumber);
                if (blockOpt.isPresent()) {
                    LOGGER.log(DEBUG, "Successfully returning block number: {0}", blockNumber);
                    metricsService.get(SingleBlocksRetrieved).increment();

                    return SingleBlockResponseUnparsed.newBuilder()
                            .status(SingleBlockResponseCode.READ_BLOCK_SUCCESS)
                            .block(blockOpt.get())
                            .build();
                } else {
                    LOGGER.log(DEBUG, "Block number {0} not found", blockNumber);
                    metricsService.get(SingleBlocksNotFound).increment();

                    return SingleBlockResponseUnparsed.newBuilder()
                            .status(SingleBlockResponseCode.READ_BLOCK_NOT_FOUND)
                            .build();
                }
            } catch (IOException e) {
                LOGGER.log(ERROR, "Error reading block number: {0}", blockNumber);

                return SingleBlockResponseUnparsed.newBuilder()
                        .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                        .build();
            } catch (ParseException e) {
                LOGGER.log(ERROR, "Error parsing block number: {0}", blockNumber);

                return SingleBlockResponseUnparsed.newBuilder()
                        .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                        .build();
            }
        } else {
            LOGGER.log(ERROR, "Unary singleBlock gRPC method is not currently running");

            return SingleBlockResponseUnparsed.newBuilder()
                    .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                    .build();
        }
    }

    @NonNull
    private SingleBlockRequest parseSingleBlockRequest(@NonNull final Bytes message) throws ParseException {
        return SingleBlockRequest.PROTOBUF.parse(message);
    }

    @NonNull
    private Bytes createSingleBlockResponse(
            @NonNull final SingleBlockResponseUnparsed reply, @NonNull final RequestOptions options) {
        return SingleBlockResponseUnparsed.PROTOBUF.toBytes(reply);
    }
}
