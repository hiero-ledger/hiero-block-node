// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.archive.ArchivePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.cloud.archive {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive org.hiero.block.node.spi;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            ArchivePlugin;
}
