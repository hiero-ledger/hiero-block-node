// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * A configurable {@link ApplicationStateFacility} test fixture. The connection sets
 * ({@code knownPublishers}, {@code inboundPartners}, {@code outboundPartners}, {@code backfillSources})
 * default to {@link #DEFAULT_NETWORK_DATA} — non-empty sample data — and can be overridden via the
 * setters. The block-range and TSS/address-book mutators are no-ops.
 */
public class TestApplicationStateFacility implements ApplicationStateFacility {

    /** Non-empty sample network data used as the default for every connection set. */
    public static final NetworkData DEFAULT_NETWORK_DATA = NetworkData.newBuilder()
            .activeEndpoints(NetworkConnection.newBuilder()
                    .local(new ConnectionReference("$", "*"))
                    .remote(new ConnectionReference("test.example.com", "40840"))
                    .category("partner")
                    .scheme("https")
                    .protocol(IpProtocol.TCP)
                    .tlsRequired(true)
                    .certificate(Bytes.EMPTY)
                    .build())
            .build();

    private NetworkData knownPublishers = DEFAULT_NETWORK_DATA;
    private NetworkData inboundPartners = DEFAULT_NETWORK_DATA;
    private NetworkData outboundPartners = DEFAULT_NETWORK_DATA;
    private NetworkData backfillSources = DEFAULT_NETWORK_DATA;

    public void setKnownPublishers(NetworkData value) {
        this.knownPublishers = value;
    }

    public void setInboundPartners(NetworkData value) {
        this.inboundPartners = value;
    }

    public void setOutboundPartners(NetworkData value) {
        this.outboundPartners = value;
    }

    public void setBackfillSources(NetworkData value) {
        this.backfillSources = value;
    }

    @Override
    public void updateTssData(TssData tssData) {
        // no-op
    }

    @Override
    public boolean updateAddressBook(NodeAddressBook nodeAddressBook) {
        return false;
    }

    @Override
    public void addStoredBlockRange(LongRange blockRange) {
        // no-op
    }

    @Override
    public void addAvailableBlockRange(LongRange blockRange) {
        // no-op
    }

    @Override
    public NetworkData knownPublishers() {
        return knownPublishers;
    }

    @Override
    public NetworkData inboundPartners() {
        return inboundPartners;
    }

    @Override
    public NetworkData outboundPartners() {
        return outboundPartners;
    }

    @Override
    public NetworkData backfillSources() {
        return backfillSources;
    }

    @Override
    public void updateBackfillSources(NetworkData sources) {
        this.backfillSources = sources;
    }
}
