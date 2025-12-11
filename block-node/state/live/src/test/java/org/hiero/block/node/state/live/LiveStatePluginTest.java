// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LiveStatePlugin}.
 */
class LiveStatePluginTest {

    @Test
    void testPluginInstantiation() {
        final LiveStatePlugin plugin = new LiveStatePlugin();
        assertNotNull(plugin);
    }
}
