// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link ZipBlockArchive}.
 */
@DisplayName("ZipBlockArchive Tests")
class ZipBlockArchiveTest {
    /** The {@link SimpleInMemoryHistoricalBlockFacility} used for testing. */
    private SimpleInMemoryHistoricalBlockFacility historicalBlockProvider;
    /** The {@link BlockNodeContext} used for testing. */
    private BlockNodeContext testContext;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() {
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .withValue("files.historic.powersOfTenPerZipFileContents", "1")
                .build();
        historicalBlockProvider = new SimpleInMemoryHistoricalBlockFacility();
        testContext = new BlockNodeContext(configuration, null, null, null, historicalBlockProvider, null);
    }

    /**
     * Constructor tests for {@link ZipBlockArchive}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockArchive} does not throw an exception when the input
         * argument is valid.
         */
        @Test
        @DisplayName("Test constructor with valid input")
        void testConstructorValidInput() {
            assertThatNoException().isThrownBy(() -> new ZipBlockArchive(testContext));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockArchive} throws a {@link NullPointerException} when
         * the input argument is null.
         */
        @Test
        @DisplayName("Test constructor with null input")
        @SuppressWarnings("all")
        void testConstructorNullInput() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockArchive(null));
        }
    }
}
