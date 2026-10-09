// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/// Registers this module's configuration data types for auto-discovery.
///
/// Also declares the swirlds-state-library config records (`MerkleDbConfig`,
/// `VirtualMapConfig`, `PathsConfig`) that `VirtualMapStateLifecycleManager` requires but the
/// swirlds jars don't self-register — this plugin owns only `StateManagementConfig`. A future
/// plugin that also needs `VirtualMapStateLifecycleManager` would need to declare these same
/// three types too; that's safe to duplicate (config construction is a pure function of the
/// global property set, so declaring the same type twice just rebuilds an identical value —
/// `ConfigurationBuilder`'s registration has no uniqueness check and never throws on it).
public class StateManagementConfigExtension implements ConfigurationExtension {

    /// Explicitly defined constructor.
    public StateManagementConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(
                StateManagementConfig.class,
                com.swirlds.merkledb.config.MerkleDbConfig.class,
                com.swirlds.virtualmap.config.VirtualMapConfig.class,
                org.hiero.consensus.config.PathsConfig.class);
    }
}
