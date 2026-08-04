// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.source.ConfigSource;
import com.swirlds.config.extensions.sources.MappedConfigSource;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds the backward-compatibility {@link ConfigSource} for the per-plugin port env vars that
 * existed before they were consolidated into {@link PortsConfig} (issue #1404): {@code
 * BLOCK_ACCESS_PORT}, {@code SERVER_STATUS_PORT}, {@code PRODUCER_PORT}, and {@code SUBSCRIBER_PORT}
 * still work, feeding {@link PortsConfig}'s renamed properties instead of the deleted per-plugin
 * config records.
 * <p>
 * The returned source sits one ordinal below {@link AutomaticEnvironmentVariableConfigSource}'s
 * (300), so the new {@code PORTS_*} env var wins whenever both the old and new name are set for the
 * same plugin.
 */
public final class LegacyPortsEnvironmentConfigSource {
    /** One below {@link AutomaticEnvironmentVariableConfigSource}'s ordinal, so PORTS_* wins on conflict. */
    private static final int ORDINAL = 299;

    /** old env var name to the {@link PortsConfig} property it now feeds. */
    private static final Map<String, String> LEGACY_ENV_TO_PORTS_PROPERTY = Map.of(
            "BLOCK_ACCESS_PORT", "ports.blockAccess",
            "SERVER_STATUS_PORT", "ports.serverStatus",
            "PRODUCER_PORT", "ports.publisher",
            "SUBSCRIBER_PORT", "ports.subscriber");

    private LegacyPortsEnvironmentConfigSource() {}

    /**
     * Creates the source, reading legacy values from the real process environment.
     *
     * @return the config source
     */
    @NonNull
    public static ConfigSource create() {
        return create(System::getenv);
    }

    /**
     * Creates the source, reading legacy values via {@code envVarGetter}.
     *
     * @param envVarGetter the function to get an environment variable value, exposed for testing
     * @return the config source
     */
    @NonNull
    static ConfigSource create(@NonNull final Function<String, String> envVarGetter) {
        final SimpleConfigSource legacyValues = new SimpleConfigSource().withOrdinal(ORDINAL);
        final MappedConfigSource mapped = new MappedConfigSource(legacyValues);
        LEGACY_ENV_TO_PORTS_PROPERTY.forEach((envName, portsProperty) -> {
            final String value = envVarGetter.apply(envName);
            // only map names actually set: an unmapped originalName is harmless (MappedConfigSource
            // just skips it), but it logs a WARN on every build() - and every plugin unset is normal.
            if (value != null) {
                legacyValues.withValue(envName, value);
                mapped.addMapping(portsProperty, envName);
            }
        });
        return mapped;
    }
}
