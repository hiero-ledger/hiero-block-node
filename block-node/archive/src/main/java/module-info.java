// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.archive.ArchivePlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.cloud.archive {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.node.spi;
    requires com.hedera.pbj.runtime;
    requires org.hiero.block.common;
    requires org.hiero.block.node.base;
    requires java.management;
    requires java.net.http;
    requires java.xml;

    provides org.hiero.block.node.spi.BlockNodePlugin with
            ArchivePlugin;
}
