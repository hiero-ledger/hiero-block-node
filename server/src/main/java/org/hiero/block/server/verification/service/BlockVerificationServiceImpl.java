// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.service;

import static java.lang.System.Logger.Level.ERROR;
import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import javax.inject.Inject;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.BlockNodeMetricTypes;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.verification.BlockVerificationStatus;
import org.hiero.block.server.verification.session.BlockVerificationSession;
import org.hiero.block.server.verification.session.BlockVerificationSessionFactory;

/**
 * Service that handles the verification of block items, it receives items from the handler.
 */
public class BlockVerificationServiceImpl implements BlockVerificationService {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final MetricsService metricsService;

    private final BlockVerificationSessionFactory sessionFactory;
    private BlockVerificationSession currentSession;
    private final AckHandler ackHandler;

    /**
     * Constructs a new BlockVerificationServiceImpl.
     *
     * @param metricsService the metrics service
     * @param sessionFactory the session factory
     */
    @Inject
    public BlockVerificationServiceImpl(
            @NonNull final MetricsService metricsService,
            @NonNull final BlockVerificationSessionFactory sessionFactory,
            @NonNull final AckHandler ackHandler) {
        this.metricsService = requireNonNull(metricsService);
        this.sessionFactory = requireNonNull(sessionFactory);
        this.ackHandler = ackHandler;
    }

    /**
     * Everytime the handler receives block items, it will call this method to verify the block items.
     *
     * @param blockItems the block items to add to the verification service
     * @throws ParseException if the block items are invalid
     */
    @Override
    public void onBlockItemsReceived(@NonNull List<BlockItemUnparsed> blockItems) throws ParseException {

        final BlockItemUnparsed firstItem = blockItems.getFirst();

        // If we have a new block header, that means a new block has started
        if (firstItem.hasBlockHeader()) {
            metricsService
                    .get(BlockNodeMetricTypes.Counter.VerificationBlocksReceived)
                    .increment();
            BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(firstItem.blockHeader());

            // start new session and set it as current
            currentSession = sessionFactory.createSession(blockHeader);
            currentSession.appendBlockItems(blockItems);

            // Handle promise completion for the session.
            currentSession.getVerificationResult().thenAccept(result -> {
                if (result.status().equals(BlockVerificationStatus.VERIFIED)) {
                    ackHandler.blockVerified(result.blockNumber(), result.blockHash());
                } else {
                    ackHandler.blockVerificationFailed(result.blockNumber());
                }
            });

        } else {
            if (currentSession == null) {
                // todo(452): correctly propagate this exception to the rest of the system, so it can be handled
                LOGGER.log(ERROR, "Received block items before a block header.");
                throw new IllegalStateException("Received block items before a block header.");
            }
            // Append to current session
            currentSession.appendBlockItems(blockItems);
        }
    }
}
