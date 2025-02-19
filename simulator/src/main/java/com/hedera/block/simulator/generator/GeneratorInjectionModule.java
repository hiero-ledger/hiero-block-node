// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.generator;

import com.hedera.block.simulator.config.data.BlockGeneratorConfig;
import com.hedera.block.simulator.config.types.GenerationMode;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

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
     * @param config the block stream configuration
     * @return the appropriate BlockStreamManager implementation based on configuration
     */
    @Singleton
    @Provides
    static BlockStreamManager providesBlockStreamManager(BlockGeneratorConfig config) {

        final String managerImpl = config.managerImplementation();
        final GenerationMode generationMode = config.generationMode();

        return switch (generationMode) {
            case DIR -> {
                if ("BlockAsDirBlockStreamManager".equalsIgnoreCase(managerImpl)) {
                    yield new BlockAsDirBlockStreamManager(config);
                } else if ("BlockAsFileLargeDataSets".equalsIgnoreCase(managerImpl)) {
                    yield new BlockAsFileLargeDataSets(config);
                }
                yield new BlockAsFileBlockStreamManager(config);
            }
            case CRAFT -> new CraftBlockStreamManager(config);
        };
    }
}
