// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * Interface for handling application state update notifications. Plugins that need to react to
 * changes in node-level state (TSS data, address book history, stored blocks, or available blocks)
 * implement this interface and register via
 * {@link BlockMessagingFacility#registerApplicationStateNotificationHandler}.
 *
 * <p>Each registered handler runs on its own dedicated thread. All handler methods have no-op
 * default implementations so that implementors only override the notifications they care about.
 */
public interface ApplicationStateNotificationHandler extends GatingHandler {

    /**
     * Handle a TSS data update notification. Always called on the handler's own messaging thread.
     *
     * @param notification the TSS data update notification to handle
     */
    default void handleTssDataUpdate(final TssDataNotification notification) {}

    /**
     * Handle an address book history update notification. Always called on the handler's own
     * messaging thread.
     *
     * @param notification the address book history update notification to handle
     */
    default void handleAddressBookHistoryUpdate(final AddressBookHistoryNotification notification) {}

    /**
     * Handle a stored blocks update notification. Always called on the handler's own messaging
     * thread.
     *
     * @param notification the stored blocks update notification to handle
     */
    default void handleStoredBlocksUpdate(final StoredBlocksNotification notification) {}

    /**
     * Handle an available blocks update notification. Always called on the handler's own messaging
     * thread.
     *
     * @param notification the available blocks update notification to handle
     */
    default void handleAvailableBlocksUpdate(final AvailableBlocksNotification notification) {}
}
