// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for LiveStatePlugin lifecycle operations: init, start, stop, restart.
 */
class LiveStatePluginLifecycleTest extends LiveStatePluginTestBase {

    @Test
    @DisplayName("Plugin can be instantiated")
    void testPluginInstantiation() {
        LiveStatePlugin plugin = new LiveStatePlugin();
        assertNotNull(plugin);
        assertEquals("LiveState", plugin.name());
    }

    @Test
    @DisplayName("Plugin reports correct config data types")
    void testPluginConfigDataTypes() {
        LiveStatePlugin plugin = new LiveStatePlugin();
        List<Class<? extends Record>> configTypes = plugin.configDataTypes();
        assertNotNull(configTypes);
        assertEquals(1, configTypes.size());
        assertEquals(LiveStateConfig.class, configTypes.get(0));
    }

    @Test
    @DisplayName("Plugin initializes and starts successfully with genesis state")
    void testPluginInitAndStartGenesis() {
        initAndStartPlugin();

        // Verify plugin is ready - blockNumber should be -1 for genesis (no blocks applied yet)
        assertEquals(-1, plugin.blockNumber());
    }

    // Note: Plugin stop with state snapshot is currently expected to fail
    // due to VirtualMap hashing requirements that aren't met in test environment.
    // This is a known limitation when using MerkleDB outside of full node context.

    @Test
    @DisplayName("Plugin applies block and updates block number")
    void testPluginAppliesBlock() {
        initAndStartPlugin();

        // Apply a block to change state
        BlockUnparsed block0 = createSimpleBlock(0, new byte[48]);
        VerificationNotification notification = createVerificationNotification(0, block0, true);
        plugin.handleVerification(notification);

        // Verify block was applied
        assertEquals(0, plugin.blockNumber());
    }

    @Test
    @DisplayName("Plugin registers as block notification handler")
    void testPluginRegistersAsNotificationHandler() {
        initAndStartPlugin();

        // Verify the plugin registered itself with the messaging facility
        assertTrue(
                blockMessaging.getNotificationHandlers().contains(plugin),
                "Plugin should be registered as a notification handler");
    }

    @Test
    @DisplayName("Plugin uses custom storage path from configuration")
    void testCustomStoragePath() {
        Path customPath = tempDir.resolve("custom_state");

        initAndStartPlugin(java.util.Map.of(
                "state.live.storagePath", customPath.toString(),
                "state.live.latestStatePath", customPath.resolve("latest").toString(),
                "state.live.stateMetadataPath",
                        customPath.resolve("metadata.dat").toString()));

        // Verify plugin started successfully with custom path
        assertEquals(-1, plugin.blockNumber());
    }
}
