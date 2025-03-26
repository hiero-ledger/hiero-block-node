// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import dagger.Module;
import dagger.Provides;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Singleton;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.types.GenerationMode;

/** The module used to inject the block stream manager. */
@Module
public interface GeneratorInjectionModule {

    /**
     * Provides the block stream manager based on the configuration settings.
     * For DIR generation mode:
     * - BlockAsDirBlockStreamManager if explicitly configured
     * - BlockAsFileLargeDataSets if explicitly configured
     * - BlockAsFileBlockStreamManager as default
     * For CRAFT generation mode:
     * - CraftBlockStreamManager
     *
     * @param generatorConfig the block stream configuration
     * @return the appropriate BlockStreamManager implementation based on configuration
     */
    @Singleton
    @Provides
    static BlockStreamManager providesBlockStreamManager(
            @NonNull final BlockGeneratorConfig generatorConfig, @NonNull final BlockStreamConfig blockStreamConfig) {
        final String managerImpl = generatorConfig.managerImplementation();
        final GenerationMode generationMode = generatorConfig.generationMode();
        return switch (generationMode) {
            case DIR -> {
                if ("BlockAsDirBlockStreamManager".equalsIgnoreCase(managerImpl)) {
                    yield new BlockAsDirBlockStreamManager(generatorConfig);
                } else if ("BlockAsFileLargeDataSets".equalsIgnoreCase(managerImpl)) {
                    yield new BlockAsFileLargeDataSets(generatorConfig);
                }
                yield new BlockAsFileBlockStreamManager(generatorConfig);
            }
            case CRAFT -> new CraftBlockStreamManager(generatorConfig, blockStreamConfig);
        };
    }
}
