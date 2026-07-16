// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;

/** Registers the application-wide configuration data types for auto-discovery. */
public class AppConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public AppConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(ServerConfig.class, WebServerHttp2Config.class, NodeConfig.class, ApplicationStateConfig.class);
    }
}
