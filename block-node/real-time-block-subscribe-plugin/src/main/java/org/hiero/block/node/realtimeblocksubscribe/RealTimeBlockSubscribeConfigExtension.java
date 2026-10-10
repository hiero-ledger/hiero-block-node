// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.realtimeblocksubscribe;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/** Registers this module's configuration data types for auto-discovery. */
public class RealTimeBlockSubscribeConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public RealTimeBlockSubscribeConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(RealTimeBlockSubscribeConfiguration.class);
    }
}
