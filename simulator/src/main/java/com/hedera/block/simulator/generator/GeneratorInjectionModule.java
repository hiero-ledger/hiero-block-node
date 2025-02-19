// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.generator;

import com.hedera.block.simulator.config.data.BlockGeneratorConfig;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

/** The module used to inject the block stream manager. */
@Module
public interface GeneratorInjectionModule {

    /**
     * Provides the block stream manager based on the configuration, either
     * BlockAsFileBlockStreamManager or BlockAsDirBlockStreamManager. by default, it will be
     * BlockAsFileBlockStreamManager.
     *
     * @param config the block stream configuration
     * @return the block stream manager
     */
    @Singleton
    @Provides
    static BlockStreamManager providesBlockStreamManager(BlockGeneratorConfig config) {

        final String managerImpl = config.managerImplementation();
        if ("BlockAsDirBlockStreamManager".equalsIgnoreCase(managerImpl)) {
            return new BlockAsDirBlockStreamManager(config);
        } else if ("BlockAsFileLargeDataSets".equalsIgnoreCase(managerImpl)) {
            return new BlockAsFileLargeDataSets(config);
        }

        return new BlockAsFileBlockStreamManager(config);
    }
}
