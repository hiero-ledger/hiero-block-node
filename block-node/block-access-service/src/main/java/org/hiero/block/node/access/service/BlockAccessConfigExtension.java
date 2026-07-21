// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/** Registers this module's configuration data types for auto-discovery. */
public class BlockAccessConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public BlockAccessConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(BlockAccessConfig.class);
    }
}
