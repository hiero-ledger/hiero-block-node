// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.BlockVerificationSession;

// todo(1661) there is a follow-up task to implement this class based on the HAPI 0.68.0 expected spec (latest)
public class ExtendedMerkleTreeVerificationSessionV0680 implements BlockVerificationSession {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    public ExtendedMerkleTreeVerificationSessionV0680(
            final long blockNumber, final BlockSource blockSource, final String extraBytes) {
        LOGGER.log(
                TRACE,
                "ExtendedMerkleTreeVerificationSessionV0680 created for block number: %d from source: %s with extra bytes: %s"
                        .formatted(blockNumber, blockSource, extraBytes));
    }

    @Override
    public VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {
        LOGGER.log(
                INFO,
                "HAPI VERSION NOT IMPLEMENTED YET. %d block items in ExtendedMerkleTreeVerificationSessionV0680"
                        .formatted(blockItems.size()));

        throw new UnsupportedOperationException("Not implemented yet");
    }
}
