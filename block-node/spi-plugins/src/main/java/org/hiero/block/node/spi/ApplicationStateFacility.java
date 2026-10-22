// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.hapi.node.base.NodeAddressBook;
import java.util.List;
import java.util.Optional;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

/**
 * Interface for the Application and block node plugins to exchange state information. The `ApplicationStateFacility`
 * is itself a {@link BlockNodePlugin}, provided by the {@code facility-app-state} module, and is passed to all block
 * node plugins in the BlockNodeContext.
 * */
public interface ApplicationStateFacility extends BlockNodePlugin {
    /**
     * Used by plugins to update the TssData for this application. i.e. {@code TssBootstrapPlugin}, and
     * {@code VerificationPlugin}. The update will be forwarded to all registered
     * {@link org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler} instances.
     *
     * @param tssData the TssData to update
     */
    void updateTssData(TssData tssData);

    /**
     * Used by plugins to update the block-number-keyed RSA address book history for this
     * application. When present, the history takes precedence over the single
     * {@code NodeAddressBook} for historical WRB verification. The update will be forwarded to
     * all registered {@link org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler}
     * instances.
     *
     * <p>The default implementation is a no-op that returns {@code false}. Implementations that
     * support the history file (i.e. {@code ApplicationStateFacilityPlugin}) override this method.
     *
     * @param history the {@code RangedAddressBookHistory} to store; must not be {@code null}
     * @return {@code true} if the history is accepted and dispatched, {@code false} if it was not
     *     (e.g. equal to the currently stored value or implementation does not support history)
     */
    boolean updateAddressBookHistory(RangedAddressBookHistory history);

    /**
     * Records a contiguous range of blocks as stored (persisted but not necessarily retrievable by
     * clients). Block availability for clients is tracked through
     * {@link #addAvailableBlockRange}.
     *
     * @param blockRange the contiguous range of block numbers being reported
     */
    void addStoredBlockRange(BlockRange blockRange);

    /**
     * Returns the {@link NodeAddressBook} for the supplied {@code blickNum }
     *
     * @param blockNum the block number whose address book you need
     * @return the {@link NodeAddressBook} or null if not found
     */
    NodeAddressBook getAddressBookForBlock(long blockNum);

    /**
     * The TSS data currently held by the application. Plugins that need the TSS data loaded at startup read it here in
     * {@code start()}; later changes are also dispatched as
     * {@link org.hiero.block.node.spi.blockmessaging.TssDataNotification}.
     *
     * @return the current TSS data, or {@code null} if none has been loaded or reported
     */
    TssData tssData();

    /**
     * The RSA address book history currently held by the application. Plugins that need the history
     * loaded at startup read it here in {@code start()}; later changes are also dispatched as
     * {@link org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification}.
     *
     * @return the current history, or {@code null} if none has been loaded or reported
     */
    RangedAddressBookHistory rangedAddressBookHistory();

    /**
     * The stored block ranges (stored ranges merged with the available blocks) currently held by the
     * application. Plugins that need the ranges loaded at startup read them here in {@code start()};
     * later changes are also dispatched as
     * {@link org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification}.
     *
     * @return the stored block ranges in ascending order; never {@code null} (empty when none)
     */
    List<BlockRange> storedBlocks();

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
     * The next block expected from publishers. This is approximate, but should
     * be greater than the latest available block, in most situations, but may
     * differ significantly if `EarliestManagedBlock` is configured or under
     * some other conditions.
     *
     * @return the current value of the next expected block
     */
    long nextExpectedBlock();

    /**
     * The next block expected from publishers, as last reported by the publisher plugin after it
     * told a publisher this node is behind it. This value is held in memory only, it only moves
     * forward, and it <b>must</b> be {@code -1} at startup, before any publisher has been answered.
     *
     * @return the next expected publisher block, or {@code -1} if no publisher has been answered yet
     */
    long nextExpectedPublisherBlock();

    /**
     * Registers (or replaces) the set of backfill source connections. Called by the backfill plugin
     * when it loads its sources, so that all connection information is owned by the Application State
     * facility rather than read directly from backfill configuration.
     *
     * @param sources the backfill source connections to publish
     */
    void updateBackfillSources(NetworkData sources);

    /**
     * Update the current value of the next expected block.
     * This value is ephemeral, and is not written to, or read from, persistent
     * storage.
     *
     * @param updatedExpectedBlock The new value for the next expected block.
     */
    void updateExpectedBlock(final long updatedExpectedBlock);

    /**
     * Used by block provider plugins to report blocks they now provide. The range is recorded against the
     * provider, added to the stored blocks too, and, if the union of available blocks changed, an
     * {@link org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification} is sent. Providers call this
     * from {@code init()} for the blocks they hold at startup, and later whenever they gain blocks.
     *
     * @param provider the plugin providing the blocks
     * @param blockRange the contiguous range of block numbers now provided
     */
    void addAvailableBlockRange(BlockProviderPlugin provider, BlockRange blockRange);

    /**
     * Used by block provider plugins to report that they no longer provide a range of blocks. A block remains
     * available if any other provider still provides it. The blocks stay in the stored blocks.
     *
     * @param provider the plugin that no longer provides the blocks
     * @param blockRange the contiguous range of block numbers no longer provided
     */
    void removeAvailableBlockRange(BlockProviderPlugin provider, BlockRange blockRange);

    /**
     * The block ranges currently available from at least one provider.
     *
     * @return the available block ranges in ascending order; never {@code null} (empty when none)
     */
    List<BlockRange> availableBlocks();

    /**
     * The same as {@link #availableBlocks()} but as a {@link BlockRangeSet} for querying
     * {@code min()}, {@code max()} and {@code contains()}.
     *
     * @return a snapshot set of the available blocks; never {@code null}
     */
    BlockRangeSet availableBlockSet();

    /**
     * The plugin providing the given block. If several plugins provide it, the one with the highest
     * {@link BlockProviderPlugin#defaultPriority()} is returned.
     *
     * @param blockNumber the block number
     * @return the providing plugin, or empty if no plugin provides the block
     */
    Optional<BlockProviderPlugin> blockProvider(long blockNumber);
}
