// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import java.util.List;
import org.hiero.block.server.notifier.Notifiable;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * Use this interface to combine the contract for mediating the live stream of blocks from the
 * Hedera network with the contract to be notified of critical system events.
 */
public interface LiveStreamMediator
        extends StreamMediator<List<BlockItemUnparsed>, List<BlockItemUnparsed>>, Notifiable {}
