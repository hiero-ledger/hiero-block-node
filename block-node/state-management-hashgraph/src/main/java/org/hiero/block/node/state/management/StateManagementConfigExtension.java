// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/// Registers this module's configuration data types for auto-discovery.
///
/// Also declares `MerkleDbConfig`/`VirtualMapConfig`, which `VirtualMapStateLifecycleManager`
/// requires but the swirlds jars don't self-register.
///
/// These records are global: one instance per process, shared by any other plugin using
/// `VirtualMapStateLifecycleManager`. Independently-configured instances (e.g. separate
/// storage paths) aren't possible, so only one such plugin can usefully run at a time.
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
                com.swirlds.virtualmap.config.VirtualMapConfig.class);
    }
}
