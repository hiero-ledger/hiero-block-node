// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.base.ArgumentUtils;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.source.ConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.RecordComponent;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Config source that automatically maps environment variables to configuration properties based on micro profile
 * standard name mapping style. For example "persistence.storage.archiveRootPath" property maps to
 * "PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH" environment variable.
 */
public class AutomaticEnvironmentVariableConfigSource implements ConfigSource {
    /** Ordinal for system environment. */
    private static final int SYSTEM_ENVIRONMENT_ORDINAL = 300;
    /** map from property name to environment variable name */
    private final Map<String, String> propertyNameToEnvMap;
    /** Set of properties that are set in the environment */
    private final Set<String> propertiesSetInEnvironment;

    /**
     * Creates a new AutomaticEnvironmentVariableConfigSource instance.
     *
     * @param configTypes the configuration types to collect property names from
     */
    public AutomaticEnvironmentVariableConfigSource(@NonNull final List<Class<? extends Record>> configTypes) {
        final Map<String, String> envToPropertyNameMap = collectEnvToPropertyNameMappings(configTypes);
        propertyNameToEnvMap = envToPropertyNameMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        propertiesSetInEnvironment = System.getenv().keySet().stream()
                .filter(envToPropertyNameMap::containsKey)
                .map(envToPropertyNameMap::get)
                .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public Set<String> getPropertyNames() {
        return propertiesSetInEnvironment;
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public String getValue(@NonNull String propertyName) throws NoSuchElementException {
        ArgumentUtils.throwArgBlank(propertyName, "propertyName");
        final String envName = propertyNameToEnvMap.get(propertyName);
        // now look up the property name in the environment variables
        if (envName == null) {
            throw new NoSuchElementException("Property " + propertyName + " is not defined");
        }
        return System.getenv(envName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isListProperty(@NonNull String propertyName) throws NoSuchElementException {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<String> getListValue(@NonNull String propertyName) throws NoSuchElementException {
        return List.of();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getOrdinal() {
        return SYSTEM_ENVIRONMENT_ORDINAL;
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public String getName() {
        return ConfigSource.super.getName();
    }

    /**
     * Scan all configured data types to find super set of all configuration properties. Then produce a mapping of
     * environment variable names to property names.
     *
     * @param configTypes the configuration types to collect property names from
     * @return sorted map of properties and values, with sensitive values masked
     */
    @NonNull
    static Map<String, String> collectEnvToPropertyNameMappings(
            @NonNull final List<Class<? extends Record>> configTypes) {
        final Map<String, String> envMappings = new TreeMap<>();
        // Iterate over all the configuration data types
        for (Class<? extends Record> configType : configTypes) {
            // Only log record components that are annotated with @ConfigData
            final ConfigData configDataAnnotation = configType.getDeclaredAnnotation(ConfigData.class);
            if (configDataAnnotation != null) {
                // For each record component, check the field annotations
                for (RecordComponent component : configType.getRecordComponents()) {
                    if (component.isAnnotationPresent(ConfigProperty.class)) {
                        final String fieldName = component.getName();
                        envMappings.put(
                                getEnvName(configDataAnnotation.value(), fieldName),
                                configDataAnnotation.value() + "." + fieldName);
                    }
                }
            }
        }
        return envMappings;
    }

    /**
     * Convert property names into environment variable names. The mapping is based on standard from micro profile. The
     * environment variable name is created by replacing all '.' with '_' and prefixing all uppercase characters with
     * '_' then converting to upper case.
     *
     * @see <a href="https://smallrye.io/smallrye-config/Main/config/environment-variables/#system-properties">Smallrye Config</a>
     * @param configDataName the name of the configuration data type
     * @param propertyName the name of the property
     * @return the environment variable name
     */
    static String getEnvName(@NonNull final String configDataName, @NonNull final String propertyName) {
        return configDataName.replace('.', '_').toUpperCase() + "_"
                + propertyName.replaceAll("([A-Z])", "_$1").toUpperCase();
    }
}
