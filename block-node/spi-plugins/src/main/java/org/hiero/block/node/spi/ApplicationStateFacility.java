// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.hapi.node.base.NodeAddressBook;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Interface for the Application and block node plugins to exchange state information. The `ApplicationStateFacility`
 * is passed to all block node plugins in the BlockNodeContext.
 * */
public interface ApplicationStateFacility {
    /**
     * Used by plugins to update the TssData for this application. i.e. `TssBootstrapPlugin`, and `VerificationPlugin`
     * The update will be forwarded to all plugins using the BlockNodePlugin.onContextUpdate() of the plugins
     *
     * @param tssData - The TssData to be updated on the `BlockNodeContext`
     * */
    void updateTssData(TssData tssData);

    /**
     * Used by plugins to update the block-number-keyed RSA address book history for this
     * application. When present, the history takes precedence over the single
     * {@code NodeAddressBook} for historical WRB verification. The update will be forwarded to
     * all plugins using {@link BlockNodePlugin#onContextUpdate(BlockNodeContext)}.
     *
     * <p>The default implementation is a no-op that returns {@code false}. Implementations that
     * support the history file (i.e. {@code BlockNodeApp}) override this method.
     *
     * @param history the {@code RangedAddressBookHistory} to store in the BlockNodeContext;
     *     must not be {@code null}
     * @return {@code true} if the history is queued for update, {@code false} if it was not
     *     (e.g. equal to the currently stored value or implementation does not support history)
     */
    boolean updateAddressBookHistory(RangedAddressBookHistory history);

    /**
     * Records a contiguous range of blocks as stored (persisted but not necessarily retrievable by
     * clients). Block availability for clients is tracked by {@code HistoricalBlockFacility}.
     *
     * @param blockRange the contiguous range of block numbers being reported
     */
    void addStoredBlockRange(LongRange blockRange);

    /**
     * Returns the {@link NodeAddressBook} for the supplied {@code blickNum }
     *
     * @param blockNum the block number whose address book you need
     * @return the {@link NodeAddressBook} or null if not found
     */
    NodeAddressBook getAddressBookForBlock(long blockNum);

    /**
     * The set of known inbound publishers, loaded from configuration on startup. Reported by the
     * {@code /statusz/inbound} endpoint.
     *
     * @return the known publishers; never {@code null} (empty when none are configured)
     */
    NetworkData knownPublishers();

    /**
     * The set of designated inbound partners, loaded from configuration on startup. Reported by the
     * {@code /statusz/inbound} endpoint.
     *
     * @return the inbound partners; never {@code null} (empty when none are configured)
     */
    NetworkData inboundPartners();

    /**
     * The set of designated outbound partners, loaded from configuration on startup. Reported by the
     * {@code /statusz/outbound} endpoint.
     *
     * @return the outbound partners; never {@code null} (empty when none are configured)
     */
    NetworkData outboundPartners();

    /**
     * The set of backfill source connections most recently reported by the backfill plugin. Backfill
     * sources are reported by <b>both</b> the {@code /statusz/inbound} and {@code /statusz/outbound}
     * endpoints, because every backfill source may also backfill from this node.
     *
     * @return the backfill sources; never {@code null} (empty when none are configured)
     */
    NetworkData backfillSources();

    /**
     * Registers (or replaces) the set of backfill source connections. Called by the backfill plugin
     * when it loads its sources, so that all connection information is owned by the Application State
     * facility rather than read directly from backfill configuration.
     *
     * @param sources the backfill source connections to publish
     */
    void updateBackfillSources(NetworkData sources);
}
