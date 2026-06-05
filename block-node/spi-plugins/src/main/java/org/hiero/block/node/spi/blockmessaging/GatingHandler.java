// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

public interface GatingHandler {
    default boolean isGating() {
        return true;
    }
}
