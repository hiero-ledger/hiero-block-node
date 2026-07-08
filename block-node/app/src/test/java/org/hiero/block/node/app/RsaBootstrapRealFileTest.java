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
import java.util.List;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
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

    /// previewnet's real fixture is a single era: startBlock 0, an open-ended endBlock (-1), and 7
    /// node addresses.
    @Test
    @DisplayName("Real previewnet roster fixture (1 era) loads via ApplicationStateFacility")
    void previewnetFixtureLoadsSuccessfully() throws Exception {
        final RangedAddressBookHistory history = loadRealFixture("/previewnet/rsa-bootstrap-roster.json");
        assertEquals(1, history.addressBooks().size(), "previewnet fixture has exactly one era");
        final RangedNodeAddressBook era = history.addressBooks().getFirst();
        assertEquals(0L, era.startBlock());
        assertEquals(-1L, era.endBlock(), "era must be open-ended");
        assertEquals(7, era.addressBook().nodeAddress().size(), "previewnet era has 7 node addresses");
    }

    /// testnet's real fixture spans 28 eras with real, ascending, non-overlapping block ranges
    /// (its RSA roster has rotated many times), from block 0 up to an open-ended era starting at
    /// block 31090489. Each era's address book has 7 node addresses, except two eras (a node
    /// temporarily joining before another left) which have 8.
    @Test
    @DisplayName("Real testnet roster fixture (28 eras) loads via ApplicationStateFacility")
    void testnetFixtureLoadsSuccessfully() throws Exception {
        final RangedAddressBookHistory history = loadRealFixture("/testnet/rsa-bootstrap-roster.json");
        assertEquals(28, history.addressBooks().size(), "testnet fixture has 28 eras");

        final long[][] expectedRanges = {
            {0, 491},
            {492, 12313873},
            {12313874, 15116048},
            {15116049, 21766582},
            {21766583, 22061130},
            {22061131, 23019378},
            {23019379, 23270496},
            {23270497, 24344217},
            {24344218, 24479727},
            {24479728, 25597790},
            {25597791, 26250788},
            {26250789, 28021425},
            {28021426, 28023538},
            {28023539, 28323628},
            {28323629, 28323658},
            {28323659, 28325877},
            {28325878, 29533103},
            {29533104, 29533133},
            {29533134, 29535352},
            {29535353, 31088421},
            {31088422, 31088451},
            {31088452, 31088480},
            {31088481, 31088512},
            {31088513, 31088541},
            {31088542, 31088570},
            {31088571, 31088600},
            {31088601, 31090488},
            {31090489, -1}
        };
        final int[] expectedNodeCounts = {
            7, 7, 7, 7, 8, 7, 8, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
        };

        final List<RangedNodeAddressBook> eras = history.addressBooks();
        for (int i = 0; i < expectedRanges.length; i++) {
            final RangedNodeAddressBook era = eras.get(i);
            assertEquals(expectedRanges[i][0], era.startBlock(), "era " + i + " startBlock");
            assertEquals(expectedRanges[i][1], era.endBlock(), "era " + i + " endBlock");
            assertEquals(expectedNodeCounts[i], era.addressBook().nodeAddress().size(), "era " + i + " node count");
        }
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
