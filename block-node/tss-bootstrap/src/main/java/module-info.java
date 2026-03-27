// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.tss.bootstrap.TssBootstrapPlugin;

module org.hiero.block.node.tss.bootstrap {
    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            TssBootstrapPlugin;
}
