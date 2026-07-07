// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Loads the real-world RSA bootstrap roster fixtures — bundled under
/// {@code src/test/resources/previewnet/rsa-bootstrap-roster.json} and
/// {@code src/test/resources/testnet/rsa-bootstrap-roster.json} — through the actual
/// {@code ApplicationStateFacility} startup path ({@link BlockNodeApp#startApplicationStateFacility()}),
/// in contrast to the synthetic in-memory histories built elsewhere in {@link BlockNodeAppTest}.
class RsaBootstrapRealFileTest {

    private BlockNodeApp app;

    @AfterEach
    void cleanup() {
        if (app != null) {
            app.stopApplicationStateFacility();
        }
        try {
            Files.deleteIfExists(Path.of("build/resources/test/data/config/rsa-bootstrap-roster.json"));
        } catch (IOException e) {
            // ignore
        }
    }

    /// previewnet's real fixture is a single, still-open era starting at block 1065122.
    @Test
    @DisplayName("Real previewnet roster fixture (1 era) loads via ApplicationStateFacility")
    void previewnetFixtureLoadsSuccessfully() throws Exception {
        final RangedAddressBookHistory history = loadRealFixture("/previewnet/rsa-bootstrap-roster.json");
        assertEquals(1, history.addressBooks().size(), "previewnet fixture has exactly one era");
        assertEquals(1065122L, history.addressBooks().getFirst().startBlock());
        assertEquals(-1L, history.addressBooks().getFirst().endBlock(), "single era must be open-ended");
    }

    /// testnet's real fixture spans 28 eras (its RSA roster has rotated many times), from block 0
    /// up to an open-ended era starting at block 31090489.
    @Test
    @DisplayName("Real testnet roster fixture (28 eras) loads via ApplicationStateFacility")
    void testnetFixtureLoadsSuccessfully() throws Exception {
        final RangedAddressBookHistory history = loadRealFixture("/testnet/rsa-bootstrap-roster.json");
        assertEquals(28, history.addressBooks().size(), "testnet fixture has 28 eras");
        assertEquals(0L, history.addressBooks().getFirst().startBlock());
        assertEquals(31090489L, history.addressBooks().getLast().startBlock());
        assertEquals(-1L, history.addressBooks().getLast().endBlock(), "last era must be open-ended");
    }

    /// Copies the given classpath fixture into the app's configured RSA bootstrap path and starts
    /// the ApplicationStateFacility against it, returning the resulting history.
    private RangedAddressBookHistory loadRealFixture(String classpathResource) throws Exception {
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        final Path rsaPath = app.blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .rsaBootstrapFilePath();
        Files.createDirectories(rsaPath.getParent());
        Files.copy(resourcePath(classpathResource), rsaPath, StandardCopyOption.REPLACE_EXISTING);

        assertDoesNotThrow(app::startApplicationStateFacility, "Real roster fixture must load without error");

        final RangedAddressBookHistory history = app.blockNodeContext.rangedAddressBookHistory();
        assertNotNull(history, "History must be populated from the real fixture");
        return history;
    }

    private static Path resourcePath(String classpathResource) throws URISyntaxException {
        final URL url = RsaBootstrapRealFileTest.class.getResource(classpathResource);
        assertNotNull(url, "Missing test resource: " + classpathResource);
        return Path.of(url.toURI());
    }
}
