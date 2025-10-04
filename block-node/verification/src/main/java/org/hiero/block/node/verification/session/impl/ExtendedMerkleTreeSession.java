// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.VerificationSession;

// todo(1661) there is a follow-up task to implement this class based on the expected spec (latest)
public class ExtendedMerkleTreeSession implements VerificationSession {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    public ExtendedMerkleTreeSession(final long blockNumber, final BlockSource blockSource, final String extraBytes) {
        final String messageBase =
                "ExtendedMerkleTreeSession created for block number: %d from source: %s with extra bytes: %s";
        LOGGER.log(TRACE, messageBase.formatted(blockNumber, blockSource, extraBytes));
    }

    @Override
    public VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {
        final String messageBase =
                "Verification Process NOT IMPLEMENTED YET. %d block items in ExtendedMerkleTreeSession";
        LOGGER.log(INFO, messageBase.formatted(blockItems.size()));

        throw new UnsupportedOperationException("Not implemented yet");
    }
}
