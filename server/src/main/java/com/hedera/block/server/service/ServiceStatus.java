// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import com.hedera.block.server.block.BlockInfo;

/**
 * The ServiceStatus interface defines the contract for checking the status of the service and
 * shutting down the web server.
 */
public interface ServiceStatus {

    /**
     * Gets the latest acked block number.
     *
     * @return the latest acked block number
     */
    BlockInfo getLatestAckedBlock();

    /**
     * Sets the latest acked block number.
     *
     * @param latestAckedBlockInfo the latest acked block number
     */
    void setLatestAckedBlock(BlockInfo latestAckedBlockInfo);

    /**
     * Gets the latest received block number, when ack is skipped it might be used instead of last acked block number.
     * Also, if persistence + verification is in progress, it might be used to check if the block is already received.
     *
     * @return the latest received block number
     */
    long getLatestReceivedBlockNumber();

    /**
     * Sets the latest received block number. should be set when a block_header is received and before the first batch is placed on the ring buffer.
     *
     * @param latestReceivedBlockNumber the latest received block number
     */
    void setLatestReceivedBlockNumber(long latestReceivedBlockNumber);
}
