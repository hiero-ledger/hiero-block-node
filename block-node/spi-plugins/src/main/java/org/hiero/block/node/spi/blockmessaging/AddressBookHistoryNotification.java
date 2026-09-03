// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import org.hiero.block.api.RangedAddressBookHistory;

/**
 * Notification sent when the node's ranged address book history is updated.
 *
 * @param rangedAddressBookHistory the updated address book history
 */
public record AddressBookHistoryNotification(RangedAddressBookHistory rangedAddressBookHistory) {}
