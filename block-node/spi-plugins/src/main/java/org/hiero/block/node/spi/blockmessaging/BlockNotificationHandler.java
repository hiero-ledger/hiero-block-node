// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/// Interface for handling block notifications.
///
/// Implementations receive every [BlockNotification] published to the facility and dispatch on
/// its concrete type, typically with a pattern-matching `switch`. See
/// `docs/design/architecture/generic-block-notifications.md` for the full design and a worked
/// example.
public interface BlockNotificationHandler extends GatingHandler {
    /// Handle a block notification.
    ///
    /// This is always called on a messaging thread. Each registered notification handler has its
    /// own virtual thread.
    ///
    /// @param notification the block notification to handle
    void handleNotification(BlockNotification notification);
}
