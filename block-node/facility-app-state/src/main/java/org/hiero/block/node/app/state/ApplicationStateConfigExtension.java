// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.state;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/** Registers the application state configuration data types for auto-discovery. */
public class ApplicationStateConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public ApplicationStateConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(ApplicationStateConfig.class);
    }
}
