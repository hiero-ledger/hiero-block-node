// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.BlockVerificationSession;

public class BlockVerificationSessionAt0680 implements BlockVerificationSession {

    public BlockVerificationSessionAt0680(final long blockNumber, final BlockSource blockSource) {}

    @Override
    public VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
