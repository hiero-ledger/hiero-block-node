// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.block;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.server.ack.AckBlockStatus;

/**
 * POJO that holds information about a block while in process.
 */
public class BlockInfo {

    private final long blockNumber;
    private Bytes blockHash;
    private final AckBlockStatus ackBlockStatus;

    /**
     * Constructor.
     * @param blockNumber the block number
     */
    public BlockInfo(long blockNumber) {
        this.blockNumber = blockNumber;
        this.ackBlockStatus = new AckBlockStatus();
    }

    /**
     * Get the block number.
     * @return the block number
     */
    public long getBlockNumber() {
        return blockNumber;
    }

    /**
     * Get the block hash.
     * @return the block hash
     */
    public Bytes getBlockHash() {
        return blockHash;
    }

    /**
     * Get the block status.
     * @return the block status
     */
    public AckBlockStatus getBlockStatus() {
        return ackBlockStatus;
    }

    /**
     * Set the block hash.
     * @param blockHash the block hash
     */
    public void setBlockHash(@NonNull Bytes blockHash) {
        this.blockHash = blockHash;
    }
}
