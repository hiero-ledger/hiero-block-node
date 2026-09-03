// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.hiero.block.api.RangedAddressBookHistory;

/**
 * Notification sent when the node's ranged address book history is updated.
 *
 * @param rangedAddressBookHistory the updated address book history; {@code null} when no history
 *     has been loaded
 */
public record AddressBookHistoryNotification(@Nullable RangedAddressBookHistory rangedAddressBookHistory) {}
