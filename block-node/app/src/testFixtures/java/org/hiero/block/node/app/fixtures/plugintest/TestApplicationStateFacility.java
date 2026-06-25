// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.hapi.node.base.NodeAddressBook;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;

public class TestApplicationStateFacility implements ApplicationStateFacility {
    private final Deque<TssData> tssDataUpdates;
    private final Deque<NodeAddressBook> nodeAddressBookUpdates;

    public TestApplicationStateFacility() {
        this.tssDataUpdates = new ConcurrentLinkedDeque<>();
        this.nodeAddressBookUpdates = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void updateTssData(final TssData tssData) {
        tssDataUpdates.add(tssData);
    }

    @Override
    public boolean updateAddressBook(final NodeAddressBook nodeAddressBook) {
        return nodeAddressBookUpdates.add(nodeAddressBook);
    }

    @Override
    public void addStoredBlockRange(final LongRange blockRange) {}

    public Deque<TssData> getTssDataUpdates() {
        return tssDataUpdates;
    }

    public Deque<NodeAddressBook> getNodeAddressBookUpdates() {
        return nodeAddressBookUpdates;
    }
}
