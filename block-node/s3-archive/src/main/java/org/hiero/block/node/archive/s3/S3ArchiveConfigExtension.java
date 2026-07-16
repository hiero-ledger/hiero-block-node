// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive.s3;

import com.swirlds.config.api.ConfigurationExtension;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

/** Registers this module's configuration data types for auto-discovery. */
public class S3ArchiveConfigExtension implements ConfigurationExtension {

    /** Explicitly defined constructor. */
    public S3ArchiveConfigExtension() {
        super();
    }

    @NonNull
    @Override
    public Set<Class<? extends Record>> getConfigDataTypes() {
        return Set.of(S3ArchiveConfig.class);
    }
}
