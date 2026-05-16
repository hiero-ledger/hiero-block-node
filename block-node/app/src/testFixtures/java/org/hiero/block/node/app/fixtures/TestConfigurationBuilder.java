// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.api.converter.ConfigConverter;
import com.swirlds.config.api.source.ConfigSource;
import com.swirlds.config.api.validation.ConfigValidator;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/// Helper for use in tests and to change the config for specific tests.
/// Instance can be used per class or per test.
public class TestConfigurationBuilder {
    private final ReentrantLock configLock = new ReentrantLock();
    private Configuration configuration = null;
    private final ConfigurationBuilder builder;

    /// Creates a new instance and automatically registers all config data records
    /// (see [ConfigData]) on classpath/modulepath that are part of the packages
    /// `com.swirlds` and `com.hedera`.
    public TestConfigurationBuilder() {
        this(true);
    }

    /// Creates a new instance and automatically registers all given config data
    /// records. This call will not do a class scan for config data records on
    /// classpath/modulepath like some of the other constructors do.
    ///
    /// @param dataTypes
    public TestConfigurationBuilder(@Nullable final Class<? extends Record> dataTypes) {
        this(false);
        if (dataTypes != null) {
            builder.withConfigDataTypes(dataTypes);
        }
    }

    /// Creates a new instance and automatically registers all config data records
    /// (see [ConfigData]) on classpath/modulepath that are part of the packages
    /// `com.swirlds` and `com.hedera` if the `registerAllTypes` param is true.
    ///
    /// @param registerAllTypes if true all config data records on classpath will automatically be registered
    public TestConfigurationBuilder(final boolean registerAllTypes) {
        if (registerAllTypes) {
            builder = ConfigurationBuilder.create().autoDiscoverExtensions();
        } else {
            builder = ConfigurationBuilder.create();
        }
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param values list of values
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValues(@NonNull final String propertyName, @Nullable final List<?> values) {
        return withSource(new SimpleConfigSource(propertyName, values));
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param value the value
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValue(@NonNull final String propertyName, @Nullable final String value) {
        return withSource(new SimpleConfigSource(propertyName, value));
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param value the value
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValue(@NonNull final String propertyName, final int value) {
        return withSource(new SimpleConfigSource(propertyName, value));
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param value the value
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValue(@NonNull final String propertyName, final double value) {
        return withSource(new SimpleConfigSource(propertyName, value));
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param value the value
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValue(@NonNull final String propertyName, final long value) {
        return withSource(new SimpleConfigSource(propertyName, value));
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param value the value
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValue(@NonNull final String propertyName, final boolean value) {
        return withSource(new SimpleConfigSource(propertyName, value));
    }

    /// Sets the value for the config.
    ///
    /// @param propertyName name of the property
    /// @param value the value
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValue(@NonNull final String propertyName, @NonNull final Object value) {
        Objects.requireNonNull(value, "value must not be null");
        return withSource(new SimpleConfigSource(propertyName, value.toString()));
    }

    /// This method returns the [Configuration] instance. If the method is called
    /// for the first time the [Configuration] instance will be created. All
    /// values that have been set (see [#withValue(String, int)]) methods will
    /// be part of the config. Next to this the config will support all config
    /// data record types (see [ConfigData]) that are on the classpath.
    ///
    /// @return the created configuration
    @NonNull
    public Configuration getOrCreateConfig() {
        configLock.lock();
        try {
            if (configuration == null) {
                configuration = builder.build();
            }
            return configuration;
        } finally {
            configLock.unlock();
        }
    }

    private void checkConfigState() {
        configLock.lock();
        try {
            if (configuration != null) {
                throw new IllegalStateException("Configuration already created!");
            }
        } finally {
            configLock.unlock();
        }
    }

    /// Adds the given config source to the builder
    ///
    /// @param configSource the config source that will be added
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withSource(@NonNull final ConfigSource configSource) {
        checkConfigState();
        builder.withSource(configSource);
        return this;
    }

    /// Adds the given config converter to the builder
    ///
    /// @param converterType the type of the config converter
    /// @param converter the config converter that will be added
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public <T> TestConfigurationBuilder withConverter(
            @NonNull final Class<T> converterType, @NonNull final ConfigConverter<T> converter) {
        checkConfigState();
        builder.withConverter(converterType, converter);
        return this;
    }

    /// Adds the given config validator to the builder
    ///
    /// @param validator the config validator that will be added
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public TestConfigurationBuilder withValidator(@NonNull final ConfigValidator validator) {
        checkConfigState();
        builder.withValidator(validator);
        return this;
    }

    /// Adds the given config data type to the builder
    ///
    /// @param type the config data type that will be added
    /// @param <T> the type of the config data type
    /// @return the [TestConfigurationBuilder] instance (for fluent API)
    @NonNull
    public <T extends Record> TestConfigurationBuilder withConfigDataType(@NonNull final Class<T> type) {
        checkConfigState();
        builder.withConfigDataType(type);
        return this;
    }
}
