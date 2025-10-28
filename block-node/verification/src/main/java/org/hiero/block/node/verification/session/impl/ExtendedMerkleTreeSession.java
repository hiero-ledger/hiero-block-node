// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.VerificationSession;

// todo(1661) there is a follow-up task to implement this class based on the expected spec (latest)
public class ExtendedMerkleTreeSession implements VerificationSession {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** The block number being verified. */
    protected final long blockNumber;

    /** The source of the block, used to construct the final notification. */
    private final BlockSource blockSource;

    /**
     * The block items for the block this session is responsible for. We collect them here so we can provide the
     * complete block in the final notification.
     */
    protected final List<BlockItemUnparsed> blockItems = new ArrayList<>();

    public ExtendedMerkleTreeSession(final long blockNumber, final BlockSource blockSource, final String extraBytes) {
        this.blockNumber = blockNumber;
        this.blockSource = blockSource;
        LOGGER.log(INFO, "Created ExtendedMerkleTreeVerificationSessionV0680 for block {0}", blockNumber);
    }

    // todo(1661) implement the real logic here, for now just return true if last item has block proof.
    @Override
    public VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {
        this.blockItems.addAll(blockItems);
        LOGGER.log(TRACE, "Processed {0} block items for block {1}", blockItems.size(), blockNumber);
        if (blockItems.getLast().hasBlockProof()) {
            BlockUnparsed block =
                    BlockUnparsed.newBuilder().blockItems(this.blockItems).build();
            Bytes blockHash = Bytes.wrap("0x00");
            LOGGER.log(TRACE, "Returning always True verification notification for block {0}", blockNumber);
            return new VerificationNotification(true, blockNumber, blockHash, block, blockSource);
        }
        return null;
    }
}
