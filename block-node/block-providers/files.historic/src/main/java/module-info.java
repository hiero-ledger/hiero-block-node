// SPDX-License-Identifier: Apache-2.0
import org.hiero.block.node.blocks.files.historic.BlocksFilesHistoricPlugin;

// SPDX-License-Identifier: Apache-2.0
module org.hiero.block.node.blocks.files.historic {
    uses com.swirlds.config.api.spi.ConfigurationBuilderFactory;

    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive org.hiero.block.base;
    requires transitive org.hiero.block.node.spi;
    requires transitive org.hiero.block.stream;
    requires transitive io.helidon.common;
    requires transitive io.helidon.webserver;
    requires org.hiero.block.common;
    requires com.github.luben.zstd_jni;
    requires com.github.spotbugs.annotations;

    provides org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin with
            BlocksFilesHistoricPlugin;
}
