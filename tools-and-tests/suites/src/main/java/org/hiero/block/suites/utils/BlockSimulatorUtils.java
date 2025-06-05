// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.simulator.BlockStreamSimulatorInjectionComponent;
import org.hiero.block.simulator.DaggerBlockStreamSimulatorInjectionComponent;

/**
 * Utility class for block simulator operations.
 * Contains methods for creating and operating with block simulators.
 */
public final class BlockSimulatorUtils {
    private BlockSimulatorUtils() {
        // Prevent instantiation
    }

    /**
     * Creates a new instance of the block stream simulator with custom configuration.
     *
     * @param customConfiguration the custom configuration which will be applied to simulator upon startup
     * @return a new instance of the block stream simulator
     * @throws IOException if an I/O error occurs
     */
    public static BlockStreamSimulatorApp createBlockSimulator(@NonNull final Map<String, String> customConfiguration)
            throws IOException {
        BlockStreamSimulatorInjectionComponent DIComponent = DaggerBlockStreamSimulatorInjectionComponent.factory()
                .create(loadSimulatorConfiguration(customConfiguration));
        return DIComponent.getBlockStreamSimulatorApp();
    }

    /**
     * Creates a new instance of the block stream simulator with default configuration.
     *
     * @return a new instance of the block stream simulator
     * @throws IOException if an I/O error occurs
     */
    public static BlockStreamSimulatorApp createBlockSimulator() throws IOException {
        BlockStreamSimulatorInjectionComponent DIComponent = DaggerBlockStreamSimulatorInjectionComponent.factory()
                .create(loadSimulatorConfiguration(Collections.emptyMap()));
        return DIComponent.getBlockStreamSimulatorApp();
    }

    /**
     * Builds the desired block simulator configuration
     *
     * @return block simulator configuration
     * @throws IOException if an I/O error occurs
     */
    public static Configuration loadSimulatorConfiguration(@NonNull final Map<String, String> customProperties)
            throws IOException {
        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withSource(SystemEnvironmentConfigSource.getInstance())
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")))
                .autoDiscoverExtensions();

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            configurationBuilder.withSource(new SimpleConfigSource(key, value).withOrdinal(500));
        }

        return configurationBuilder.build();
    }
}
