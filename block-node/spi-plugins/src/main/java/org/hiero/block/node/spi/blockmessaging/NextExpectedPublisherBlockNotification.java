// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/// Sent by the publisher plugin after it told a publisher that this node is behind it
/// (`BehindPublisher`), so the next block this node expects from publishers is known even
/// before any block header has been accepted.
///
/// @param blockNumber the next block this node expects from publishers, one greater than the
///     latest block persisted by this node when the response was sent
/// @param publisherOfferedBlock the block the publisher offered, which this node could not
///     accept; kept for diagnostics only
public record NextExpectedPublisherBlockNotification(long blockNumber, long publisherOfferedBlock) {}
