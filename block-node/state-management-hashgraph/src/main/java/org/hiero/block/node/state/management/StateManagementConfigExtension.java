// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/// Registers this module's configuration data types for auto-discovery.
///
/// This also covers the swirlds-state-library config records
/// (`MerkleDbConfig`, `VirtualMapConfig`, `PathsConfig`) the plugin depends on but does not
/// itself own; see `docs/design/state/live-state.md` for the ownership follow-up tracked
/// against this plugin leaving beta.
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
