// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import com.hedera.block.server.notifier.Notifiable;
import com.hedera.hapi.block.BlockItemUnparsed;
import java.util.List;

/**
 * Use this interface to combine the contract for mediating the live stream of blocks from the
 * Hedera network with the contract to be notified of critical system events.
 */
public interface LiveStreamMediator
        extends StreamMediator<List<BlockItemUnparsed>, List<BlockItemUnparsed>>, Notifiable {}
