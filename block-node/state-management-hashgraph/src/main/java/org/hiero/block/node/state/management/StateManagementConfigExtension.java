// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/// Registers this module's configuration data types for auto-discovery.
///
/// Also declares the swirlds-state-library config records (`MerkleDbConfig`,
/// `VirtualMapConfig`, `PathsConfig`) that `VirtualMapStateLifecycleManager` requires but the
/// swirlds jars don't self-register — this plugin owns only `StateManagementConfig`.
///
/// These records are global, not per-plugin: there is exactly one instance of each for the
/// whole process. A future plugin also using `VirtualMapStateLifecycleManager` would share
/// these same instances rather than getting its own — registering the type again is harmless
/// (no uniqueness check, rebuilds an identical value), but independently-configured instances
/// (e.g. separate storage paths) aren't possible. In practice, only one plugin using the
/// swirlds-state-library can be usefully loaded at a time.
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
