// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public class TssBootstrapPlugin implements BlockNodePlugin {
    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // todo: see if the statusDetail TSS Data file exists
        // todo: get the tss information and add to context
    }
}
