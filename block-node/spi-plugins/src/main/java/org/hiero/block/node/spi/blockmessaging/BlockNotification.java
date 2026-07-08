// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/// Marker interface implemented by every notification sent through
/// [BlockMessagingFacility]'s block notification ring buffer.
///
/// Any plugin may define its own record implementing this interface to emit a
/// custom notification, without any change to `spi-plugins` or
/// `facility-messaging`. See `docs/design/architecture/generic-block-notifications.md`
/// for the full design and a worked example.
public interface BlockNotification {}
