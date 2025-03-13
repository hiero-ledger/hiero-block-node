// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.logging;

import static java.lang.System.Logger.Level.INFO;
import static java.util.Objects.requireNonNull;

import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.inject.Inject;

/**
 * Use this class to log configuration data.
 */
public final class SimulatorConfigurationLogger implements ConfigurationLogging {
    private final System.Logger LOGGER = System.getLogger(SimulatorConfigurationLogger.class.getName());
    private final Configuration configuration;
    private final Set<Record> loggablePackages = new HashSet<>();

    /**
     * Create a new instance of ConfigurationLoggingImpl.
     *
     * @param configuration the resolved configuration to log
     */
    @Inject
    public SimulatorConfigurationLogger(@NonNull final Configuration configuration) {
        this.configuration = requireNonNull(configuration);
        // The configuration properties in the following packages
        // are out of our control. So allow them to be logged.
        loggablePackages.add(configuration.getConfigData(MetricsConfig.class));
        loggablePackages.add(configuration.getConfigData(PrometheusConfig.class));
    }

    /**
     * Log the configuration data.
     */
    @Override
    public void log() {
        // FINE, FINER and FINEST will be allowed to log the configuration
        if (LOGGER.isLoggable(INFO)) {
            final Map<String, Object> config = collectConfig(configuration);
            // Header
            final String bannerLine = "=".repeat(Math.max(0, calculateMaxLineLength(config)));
            LOGGER.log(INFO, bannerLine);
            LOGGER.log(INFO, "Simulator Configuration");
            LOGGER.log(INFO, bannerLine);
            // Log the configuration
            for (final Map.Entry<String, Object> entry : config.entrySet()) {
                LOGGER.log(INFO, entry.getKey() + "=" + entry.getValue());
            }
            // Footer
            LOGGER.log(INFO, bannerLine);
        }
    }

    @NonNull
    Map<String, Object> collectConfig(@NonNull final Configuration configuration) {
        // Iterate over all the configuration data types
        final Map<String, Object> config = new TreeMap<>();
        for (final Class<? extends Record> configType : configuration.getConfigDataTypes()) {
            // Only log record components that are annotated with @ConfigData
            final ConfigData configDataAnnotation = configType.getDeclaredAnnotation(ConfigData.class);
            if (configDataAnnotation != null) {
                // For each record component, check the field annotations
                for (final RecordComponent component : configType.getRecordComponents()) {
                    if (component.isAnnotationPresent(ConfigProperty.class)) {
                        final String fieldName = component.getName();
                        final Record configRecord = configuration.getConfigData(configType);
                        try {
                            final Object value = component.getAccessor().invoke(configRecord);
                            // If the field is not annotated as '@Loggable' and it's
                            // not exempted in loggablePackages then it's a sensitive
                            // value and needs to be handled differently.
                            final boolean loggableAnnotationPresent = component.getAnnotation(Loggable.class) == null;
                            final boolean notContainedInLoggablePackages = !loggablePackages.contains(configRecord);
                            if (loggableAnnotationPresent && notContainedInLoggablePackages) {
                                // If the field is blank then log the value as a blank string
                                // to let an admin know the sensitive value was not injected.
                                // Otherwise, log the value as '*****' to mask it.
                                final String maskedValue = (value.toString().isEmpty()) ? "" : "*****";
                                config.put(configDataAnnotation.value() + "." + fieldName, maskedValue);
                            } else {
                                // Log clear text values which were annotated with
                                // '@Loggable' or which were explicitly added to
                                // loggablePackages.
                                config.put(configDataAnnotation.value() + "." + fieldName, value);
                            }
                        } catch (final IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return config;
    }

    static int calculateMaxLineLength(@NonNull final Map<String, Object> output) {
        int maxLength = 0;
        for (final Map.Entry<String, Object> entry : output.entrySet()) {
            int lineLength =
                    entry.getKey().length() + entry.getValue().toString().length();
            if (lineLength > maxLength) {
                maxLength = lineLength;
            }
        }
        return maxLength;
    }
}
