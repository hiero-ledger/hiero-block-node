// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.logging;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.node.app.logging.CleanColorfulFormatter.CYAN;
import static org.hiero.block.node.app.logging.CleanColorfulFormatter.GREY;
import static org.hiero.block.node.app.logging.CleanColorfulFormatter.WHITE;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Map;
import java.util.TreeMap;
import org.hiero.block.node.base.Loggable;

/**
 * Use this class to log configuration data.
 */
public final class ConfigLogger {
    /** The logger for this class. */
    private static final System.Logger LOGGER = System.getLogger(ConfigLogger.class.getName());
    /** banner separator line */
    private static final String BANNER_LINE = "=".repeat(120);

    /**
     * Log the configuration data.
     *
     * @param configuration the configuration to log
     */
    public static void log(@NonNull final Configuration configuration) {
        // FINE, FINER and FINEST will be allowed to log the configuration
        if (LOGGER.isLoggable(INFO)) {
            // Header
            LOGGER.log(INFO, BANNER_LINE);
            // Log loaded configuration data types
            LOGGER.log(INFO, "Configuration data types:");
            for (final var type : configuration.getConfigDataTypes()) {
                LOGGER.log(INFO, GREY + "    " + type.getName());
            }
            LOGGER.log(INFO, BANNER_LINE);
            LOGGER.log(INFO, "Combined Configuration:");
            // Log the configuration
            for (final var config : collectConfig(configuration).entrySet()) {
                LOGGER.log(INFO, GREY + "    " + config.getKey() + CYAN + " = " + WHITE + config.getValue());
            }
            // Footer
            LOGGER.log(INFO, BANNER_LINE);
        }
    }

    /**
     * Scan all configured data types to find super set of all configuration properties. Check if they are loggable or
     * sensitive and add to sorted map as appropriate.
     *
     * @param configuration the configuration to log
     * @return sorted map of properties and values, with sensitive values masked
     */
    @NonNull
    static Map<String, String> collectConfig(@NonNull final Configuration configuration) {
        final Map<String, String> config = new TreeMap<>();
        // Iterate over all the configuration data types
        for (Class<? extends Record> configType : configuration.getConfigDataTypes()) {
            // Only log record components that are annotated with @ConfigData
            final ConfigData configDataAnnotation = configType.getDeclaredAnnotation(ConfigData.class);
            if (configDataAnnotation != null) {
                // For each record component, check the field annotations
                for (RecordComponent component : configType.getRecordComponents()) {
                    if (component.isAnnotationPresent(ConfigProperty.class)) {
                        final String fieldName = component.getName();
                        final Record configRecord = configuration.getConfigData(configType);
                        try {
                            final String value =
                                    component.getAccessor().invoke(configRecord).toString();
                            // If the field is not annotated as '@Loggable' then it's a sensitive value and needs to
                            // be handled differently.
                            if (component.getAnnotation(Loggable.class) == null) {
                                // If the field is blank then log the value as a blank string
                                // to let an admin know the sensitive value was not injected.
                                // Otherwise, log the value as '*****' to mask it.
                                final String maskedValue = (value.isEmpty()) ? "" : "*****";
                                config.put(configDataAnnotation.value() + "." + fieldName, maskedValue);
                            } else {
                                // Log clear text values which were annotated with
                                // '@Loggable' or which were explicitly added to
                                // loggablePackages.
                                config.put(configDataAnnotation.value() + "." + fieldName, value);
                            }
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return config;
    }
}
