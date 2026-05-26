// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import org.hiero.block.node.spi.BlockNodePlugin;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests covering the bare module wiring of the live-state plugin. The full
 * ServiceLoader discovery path is exercised by the block-node app's plugin loader,
 * which is the correct integration boundary; here we verify the SPI implementation
 * and config contribution are wired correctly.
 */
class LiveStatePluginSkeletonTest {

    @Test
    void pluginImplementsSpiAndContributesConfig() {
        final LiveStatePlugin plugin = new LiveStatePlugin();
        assertThat(plugin).isInstanceOf(BlockNodePlugin.class);
        assertThat(plugin.configDataTypes()).containsExactly(LiveStateConfig.class);
        assertThat(plugin.name()).isEqualTo("LiveStatePlugin");
    }
}
