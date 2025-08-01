// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.hiero.block.simulator.fixtures.TestUtils.getAbsoluteFolder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.fixtures.TestUtils;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GeneratorInjectionModuleTest {
    @Mock
    private SimulatorStartupData startupDataMock;

    private final UnorderedStreamConfig unorderedStreamConfig;

    public GeneratorInjectionModuleTest() throws IOException {
        this.unorderedStreamConfig = TestUtils.getTestConfiguration(Map.of("unorderedStream.enabled", "false"))
                .getConfigData(UnorderedStreamConfig.class);
    }

    @Test
    void providesBlockStreamManager_AsFileLargeDataSets() throws IOException {
        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(Map.of(
                        "generator.generationMode",
                        "DIR",
                        "generator.folderRootPath",
                        getAbsoluteFolder("build/resources/test/block-0.0.3-blk/")))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamManager blockStreamManager = GeneratorInjectionModule.providesBlockStreamManager(
                blockGeneratorConfig, startupDataMock, unorderedStreamConfig);

        assertEquals(blockStreamManager.getClass().getName(), BlockAsFileLargeDataSets.class.getName());
    }

    @Test
    void providesBlockStreamManager_asCraft() throws IOException {
        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(
                        Map.of("generator.generationMode", "CRAFT"))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamManager blockStreamManager = GeneratorInjectionModule.providesBlockStreamManager(
                blockGeneratorConfig, startupDataMock, unorderedStreamConfig);

        assertEquals(blockStreamManager.getClass().getName(), CraftBlockStreamManager.class.getName());
    }
}
