// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/// A block node plugin that tries to get the latest TssData and makes it available to the `BlockNodeApp` and
/// to the `ServerStatusServicePlugin`
///
/// The TssData is retrieved from TssData sources in the following order:
///  - `RosterBootstrapTssConfig` TssData fields (ledgerId, wrapsVerificationKey, etc)
///  - (todo) Peer BlockNodes Queries other peer BlockNodes periodically for TssData
public class RosterBootstrapTssPlugin implements BlockNodePlugin {

    /// {@inheritDoc}
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(RosterBootstrapTssConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {

        // todo: query peer BNs for their TssData, if there is no config data, or nothing in the context
    }
}
