// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Singleton;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.startup.SimulatorStartupData;

/** The module used to inject the block stream manager. */
@Module
public interface GeneratorInjectionModule {

    /**
     * Provides the block stream manager based on the configuration settings.
     * For DIR generation mode:
     * - BlockAsFileLargeDataSets
     * For CRAFT generation mode:
     * - CraftBlockStreamManager
     *
     * @param generatorConfig the block stream configuration
     * @param simulatorStartupData simulator startup data
     * @return the appropriate BlockStreamManager implementation based on configuration
     */
    @Singleton
    @Provides
    static BlockStreamManager providesBlockStreamManager(
            @NonNull final BlockGeneratorConfig generatorConfig,
            @NonNull final SimulatorStartupData simulatorStartupData,
            @NonNull final UnorderedStreamConfig unorderedStreamConfig) {
        final GenerationMode generationMode = generatorConfig.generationMode();
        return switch (generationMode) {
            case DIR -> new BlockAsFileLargeDataSets(generatorConfig);
            case CRAFT -> new CraftBlockStreamManager(generatorConfig, simulatorStartupData, unorderedStreamConfig);
        };
    }
}
