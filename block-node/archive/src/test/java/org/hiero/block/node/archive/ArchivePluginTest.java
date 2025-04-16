package org.hiero.block.node.archive;

import org.hiero.block.node.spi.BlockNodePlugin;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link ArchivePlugin} class.
 */
class ArchivePluginTest extends PluginTestBase<ArchivePlugin> {

    @Test
    @DisplayName("ArchivePlugin should implement BlockNodePlugin interface")
    void shouldImplementBlockNodePlugin() {
        // Given
        final ArchivePlugin archivePlugin = new ArchivePlugin();

        // Then
        assertTrue(archivePlugin instanceof BlockNodePlugin, "ArchivePlugin should implement BlockNodePlugin");
    }

    @Test
    @DisplayName("ArchivePlugin should initialize without exceptions")
    void shouldInitializeWithoutExceptions() {
        // Given
        final ArchivePlugin archivePlugin = new ArchivePlugin();

        // When & Then
        assertDoesNotThrow(archivePlugin::toString, "ArchivePlugin should initialize without throwing exceptions");
    }

    @Test
    @DisplayName("ArchivePlugin should have a non-null name")
    void shouldHaveName() {
        final ArchivePlugin archivePlugin = new ArchivePlugin();
        assertNotNull(archivePlugin.name(), "name should not be null");
        assertEquals(ArchivePlugin.class.getSimpleName(), archivePlugin.name(), "Name should be class name");
    }

    @Test
    @DisplayName("ArchivePlugin should have empty config data types")
    void shouldHaveEmptyConfigDataTypes() {
        final ArchivePlugin archivePlugin = new ArchivePlugin();
        assertTrue(archivePlugin.configDataTypes().isEmpty(), "configDataTypes should be empty");
    }
}
