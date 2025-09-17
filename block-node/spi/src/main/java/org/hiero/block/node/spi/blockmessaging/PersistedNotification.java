// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

public record PersistedNotification(
        long blockNumber, boolean succeeded, int blockProviderPriority, BlockSource blockSource) {}
