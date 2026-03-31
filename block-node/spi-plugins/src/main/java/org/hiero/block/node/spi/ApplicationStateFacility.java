// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import org.hiero.block.api.TssData;

/**
 * Interface for the Application and block node plugins to exchange state information. The `ApplicationStateFacility`
 * is passed to all block node plugins in the `init()` call.
 * */
public interface ApplicationStateFacility {
    /**
     * Used by plugins to update the TssData for this application. i.e. `TssBootstrapPlugin`, and `VerificationPlugin`
     * The update will be forwarded to all plugins using the BlockNodePlugin.onContextUpdate() of the plugins
     *
     * @param tssData - The TssData to be updated on the `BlockNodeContext`
     * */
    default void updateTssData(TssData tssData) {}
}
