// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.util.Map;
import org.hiero.block.simulator.TestUtils;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.junit.jupiter.api.Test;

class GeneratorInjectionModuleTest {
    @Test
    void providesBlockStreamManager_AsFileLargeDataSets() throws IOException {

        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(Map.of(
                        "generator.generationMode",
                        "DIR",
                        "generator.managerImplementation",
                        "BlockAsFileLargeDataSets"))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamConfig blockStreamConfig = ConfigurationBuilder.create()
                .withConfigDataType(BlockStreamConfig.class)
                .withValue("useSimulatorStartupData", "false")
                .build()
                .getConfigData(BlockStreamConfig.class);

        final BlockStreamManager blockStreamManager =
                GeneratorInjectionModule.providesBlockStreamManager(blockGeneratorConfig, blockStreamConfig);

        assertEquals(blockStreamManager.getClass().getName(), BlockAsFileLargeDataSets.class.getName());
    }

    @Test
    void providesBlockStreamManager_AsFile() throws IOException {
        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(Map.of(
                        "generator.generationMode",
                        "DIR",
                        "generator.managerImplementation",
                        "BlockAsFileBlockStreamManager",
                        "generator.folderRootPath",
                        ""))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamConfig blockStreamConfig = ConfigurationBuilder.create()
                .withConfigDataType(BlockStreamConfig.class)
                .withValue("useSimulatorStartupData", "false")
                .build()
                .getConfigData(BlockStreamConfig.class);

        final BlockStreamManager blockStreamManager =
                GeneratorInjectionModule.providesBlockStreamManager(blockGeneratorConfig, blockStreamConfig);

        assertEquals(blockStreamManager.getClass().getName(), BlockAsFileBlockStreamManager.class.getName());
    }

    @Test
    void providesBlockStreamManager_AsDir() throws IOException {
        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(Map.of(
                        "generator.generationMode",
                        "DIR",
                        "generator.managerImplementation",
                        "BlockAsDirBlockStreamManager"))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamConfig blockStreamConfig = ConfigurationBuilder.create()
                .withConfigDataType(BlockStreamConfig.class)
                .withValue("useSimulatorStartupData", "false")
                .build()
                .getConfigData(BlockStreamConfig.class);

        final BlockStreamManager blockStreamManager =
                GeneratorInjectionModule.providesBlockStreamManager(blockGeneratorConfig, blockStreamConfig);

        assertEquals(
                BlockAsDirBlockStreamManager.class.getName(),
                blockStreamManager.getClass().getName());
    }

    @Test
    void providesBlockStreamManager_default() throws IOException {
        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(Map.of(
                        "generator.generationMode",
                        "DIR",
                        "generator.managerImplementation",
                        "",
                        "generator.folderRootPath",
                        ""))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamConfig blockStreamConfig = ConfigurationBuilder.create()
                .withConfigDataType(BlockStreamConfig.class)
                .withValue("useSimulatorStartupData", "false")
                .build()
                .getConfigData(BlockStreamConfig.class);

        final BlockStreamManager blockStreamManager =
                GeneratorInjectionModule.providesBlockStreamManager(blockGeneratorConfig, blockStreamConfig);

        assertEquals(blockStreamManager.getClass().getName(), BlockAsFileBlockStreamManager.class.getName());
    }

    @Test
    void providesBlockStreamManager_asCraft() throws IOException {
        final BlockGeneratorConfig blockGeneratorConfig = TestUtils.getTestConfiguration(
                        Map.of("generator.generationMode", "CRAFT"))
                .getConfigData(BlockGeneratorConfig.class);

        final BlockStreamConfig blockStreamConfig = ConfigurationBuilder.create()
                .withConfigDataType(BlockStreamConfig.class)
                .withValue("useSimulatorStartupData", "false")
                .build()
                .getConfigData(BlockStreamConfig.class);

        final BlockStreamManager blockStreamManager =
                GeneratorInjectionModule.providesBlockStreamManager(blockGeneratorConfig, blockStreamConfig);

        assertEquals(blockStreamManager.getClass().getName(), CraftBlockStreamManager.class.getName());
    }
}
