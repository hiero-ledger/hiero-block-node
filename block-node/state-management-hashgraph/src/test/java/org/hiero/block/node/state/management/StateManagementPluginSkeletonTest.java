// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import org.hiero.block.node.spi.BlockNodePlugin;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests covering the bare module wiring of the live-state plugin. The full
 * ServiceLoader discovery path is exercised by the block-node app's plugin loader,
 * which is the correct integration boundary; here we verify the SPI implementation
 * and config contribution are wired correctly.
 */
class StateManagementPluginSkeletonTest {

    @Test
    void pluginImplementsSpiAndContributesConfig() {
        final StateManagementPlugin plugin = new StateManagementPlugin();
        assertThat(plugin).isInstanceOf(BlockNodePlugin.class);
        assertThat(plugin.configDataTypes()).contains(StateManagementConfig.class);
        assertThat(plugin.name()).isEqualTo("StateManagementPlugin");
    }
}
