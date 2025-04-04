// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.swirlds.config.api.ConfigurationBuilder;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link BlockPath}.
 */
class BlockPathTest {
    /** The configuration for the test. */
    private FilesHistoricConfig defaultConfig;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() {
        defaultConfig = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }

    @Test
    @DisplayName("Test computeBlockPath with valid inputs")
    void testComputeBlockPath() {
        final long blockNumber = 123456789L;
        final BlockPath blockPath = BlockPath.computeBlockPath(defaultConfig, blockNumber);

        assertEquals(
                Paths.get("/opt/hashgraph/blocknode/data/historic/000/000/000/012/345/6000s.zip"),
                blockPath.zipFilePath());
        assertEquals("0000000000123456789", blockPath.blockNumStr());
        assertEquals("0000000000123456789.blk.zstd", blockPath.blockFileName());
    }

    @Test
    @DisplayName("Test computeBlockPath with valid input and many 19 digit block number")
    void testComputeBlockPathDifferentConfig() {

        final long blockNumber = 1234567890123456789L;
        final BlockPath blockPath = BlockPath.computeBlockPath(defaultConfig, blockNumber);

        assertEquals(
                Paths.get("/opt/hashgraph/blocknode/data/historic/123/456/789/012/345/6000s.zip"),
                blockPath.zipFilePath());
        assertEquals("1234567890123456789", blockPath.blockNumStr());
        assertEquals("1234567890123456789.blk.zstd", blockPath.blockFileName());
    }
}
