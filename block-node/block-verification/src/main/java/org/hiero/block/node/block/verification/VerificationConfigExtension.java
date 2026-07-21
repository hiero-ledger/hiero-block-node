// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/// Registers this module's configuration data types for auto-discovery.
public class VerificationConfigExtension implements ConfigurationExtension {

    /// Explicitly defined constructor.
    public VerificationConfigExtension() {
        super();
    }

    /// {@inheritDoc}
    /// ---
    /// Returns the configuration data types provided by this module.
    ///
    /// @return a set containing [VerificationConfig]
    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(VerificationConfig.class);
    }
}
