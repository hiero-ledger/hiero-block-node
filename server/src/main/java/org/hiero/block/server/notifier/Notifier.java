// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.notifier;

import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.server.mediator.StreamMediator;

/**
 * Use this interface to combine the contract for streaming block items with the contract to be
 * notified of critical system events.
 */
public interface Notifier extends StreamMediator<PublishStreamResponse, PublishStreamResponse>, Notifiable {
    /**
     * Sends an acknowledgement for the given block number and block hash.
     * @param blockNumber number of the block to ack
     * @param blockHash root hash of the block to ack
     * @param isDuplicated true if the block is a duplicate, false otherwise
     */
    void sendAck(long blockNumber, Bytes blockHash, boolean isDuplicated);

    /**
     * Sends an end of stream message for the given block number.
     * @param block_number number of the block to ack
     * @param responseCode the response code to send
     */
    void sendEndOfStream(long block_number, PublishStreamResponseCode responseCode);
}
