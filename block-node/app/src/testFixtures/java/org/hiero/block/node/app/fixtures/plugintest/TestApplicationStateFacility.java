// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.hapi.node.base.NodeAddressBook;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;

public class TestApplicationStateFacility implements ApplicationStateFacility {
    @Override
    public void updateTssData(final TssData tssData) {}

    @Override
    public boolean updateAddressBook(final NodeAddressBook nodeAddressBook) {
        return false;
    }

    @Override
    public void addStoredBlockRange(final LongRange blockRange) {}

    @Override
    public void addAvailableBlockRange(final LongRange blockRange) {}
}
