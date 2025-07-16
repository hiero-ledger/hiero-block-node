// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

public record BackfilledBlockNotification(long blockNumber, org.hiero.block.internal.BlockUnparsed block) {}
