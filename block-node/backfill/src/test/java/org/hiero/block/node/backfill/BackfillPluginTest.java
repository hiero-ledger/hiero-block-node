// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.mockito.Mockito.mock;

import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.junit.jupiter.api.Test;

class BackfillPluginTest extends PluginTestBase<BackfillPlugin> {

    public BackfillPluginTest() {
        start(new BackfillPlugin(), new NoBlocksHistoricalBlockFacility());
    }

    @Test
    void testBackfillPlugin() {
        BlockItems blockItems = mock(BlockItems.class);
        // This test is just to ensure the plugin starts without issues.
        // Additional tests for functionality should be added as needed.
    }
}
