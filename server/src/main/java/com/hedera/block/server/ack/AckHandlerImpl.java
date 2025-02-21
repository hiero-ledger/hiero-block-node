// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.ack;

import com.hedera.block.server.block.BlockInfo;
import com.hedera.block.server.metrics.BlockNodeMetricTypes;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.remove.BlockRemover;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult.BlockPersistenceStatus;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

/**
 * A simplified AckHandler that:
 *  Creates BlockInfo entries on demand when blockPersisted or blockVerified arrives.
 *  If either skipPersistence or skipVerification is true, ignores all events entirely (no ACKs).
 *  Acks blocks only in strictly increasing order
 *    the ACK is delayed until it is that block's turn.
 *    consecutive ACKs for all blocks that are both persisted and verified.
 */
public class AckHandlerImpl implements AckHandler {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final Map<Long, BlockInfo> blockInfo = new ConcurrentHashMap<>();
    private volatile long lastAcknowledgedBlockNumber = -1;
    private final Notifier notifier;
    private final boolean skipAcknowledgement;
    private final ServiceStatus serviceStatus;
    private final BlockRemover blockRemover;
    private final MetricsService metricsService;

    /**
     * Constructor. If either skipPersistence or skipVerification is true,
     * we ignore all events (no ACKs ever sent).
     */
    @Inject
    public AckHandlerImpl(
            @NonNull final Notifier notifier,
            final boolean skipAcknowledgement,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final BlockRemover blockRemover,
            @NonNull final MetricsService metricsService) {
        this.notifier = Objects.requireNonNull(notifier);
        this.skipAcknowledgement = skipAcknowledgement;
        this.serviceStatus = Objects.requireNonNull(serviceStatus);
        this.blockRemover = Objects.requireNonNull(blockRemover);
        this.metricsService = metricsService;
    }

    @Override
    public void blockPersisted(@NonNull final BlockPersistenceResult blockPersistenceResult) {
        Objects.requireNonNull(blockPersistenceResult);
        if (!skipAcknowledgement) {
            final long blockNumber = blockPersistenceResult.blockNumber();
            if (blockPersistenceResult.status() == BlockPersistenceStatus.SUCCESS) {
                final BlockInfo info =
                        blockInfo.containsKey(blockNumber) ? blockInfo.get(blockNumber) : new BlockInfo(blockNumber);
                info.getBlockStatus().setPersisted();
            } else {
                // @todo(545) handle other cases for the blockPersistenceResult
                // for now we will simply send an end of stream message
                // but more things need to be handled, like ensure the
                // blockInfo map will not be inserted
                blockVerificationFailed(blockNumber);
                blockInfo.remove(blockNumber);
            }
            attemptAcks();
        }
    }

    /**
     * Called when we receive a "verified" event for the given blockNumber,
     * with the computed blockHash.
     * @param blockNumber the block number
     * @param blockHash the block hash
     */
    @Override
    public void blockVerified(long blockNumber, @NonNull Bytes blockHash) {
        if (skipAcknowledgement) {
            return;
        }

        BlockInfo info = blockInfo.computeIfAbsent(blockNumber, BlockInfo::new);
        info.setBlockHash(blockHash);
        info.getBlockStatus().setVerified();

        attemptAcks();
    }

    /**
     * If the block verification failed, we send an end of stream message to the notifier.
     * @param blockNumber the block number that failed verification
     */
    @Override
    public void blockVerificationFailed(long blockNumber) {
        notifier.sendEndOfStream(lastAcknowledgedBlockNumber, PublishStreamResponseCode.STREAM_ITEMS_BAD_STATE_PROOF);
        try {
            blockRemover.removeLiveUnverified(blockNumber);
        } catch (IOException e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to remove block " + blockNumber, e);
            throw new RuntimeException(e);
        }
        blockInfo.remove(blockNumber);
    }

    /**
     * Attempt to ACK all blocks that are ready to be ACKed.
     * This method is called whenever a block is persisted or verified.
     * It ACKs all blocks in sequence that are both persisted and verified.
     */
    private void attemptAcks() {
        // Temporarily if lastAcknowledgedBlockNumber is -1, we get the first block in the map
        if (lastAcknowledgedBlockNumber == -1) {
            // @todo(147): once we have a way to get the last acknowledged block from the store we should use that
            lastAcknowledgedBlockNumber = 0;
        }

        // Keep ACK-ing starting from the next block in sequence
        while (true) {
            long nextBlock = lastAcknowledgedBlockNumber + 1;
            BlockInfo info = blockInfo.get(nextBlock);

            if (info == null) {
                // We have no info for the next expected block yet.
                // => We can't ACK the "next" block. Stop.
                break;
            }

            // Check if this block is fully ready
            if (!info.getBlockStatus().isPersisted() || !info.getBlockStatus().isVerified()) {
                // Not fully ready. Stop.
                break;
            }

            // Attempt to mark ACK sent (CAS-protected to avoid duplicates)
            if (info.getBlockStatus().markAckSentIfNotAlready()) {
                // We "won" the race; we do the actual ACK
                notifier.sendAck(nextBlock, info.getBlockHash(), false);

                // Update the service status
                serviceStatus.setLatestAckedBlock(info);

                // Remove from map if desired (so we don't waste memory)
                blockInfo.remove(nextBlock);

                // Update metrics and logging
                metricsService.get(BlockNodeMetricTypes.Counter.AckedBlocked).increment();
                LOGGER.log(System.Logger.Level.DEBUG, "ACKed block " + nextBlock);

                // Update last acknowledged
                lastAcknowledgedBlockNumber = nextBlock;
            } else {
                // Someone else already ACKed this block.
                // Stop, as we can't ACK the next block until this one is ACKed.
                // Also, to unblock the other thread faster.
                break;
            }
            // Loop again in case the next block is also ready.
            // This can ACK multiple consecutive blocks if they are all
            // persisted & verified in order.
        }
    }
}
