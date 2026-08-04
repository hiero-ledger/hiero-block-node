// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.api.ConfigurationExtension;
import com.swirlds.config.api.source.ConfigSource;
import java.util.Set;
import java.util.stream.Stream;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LegacyPortsEnvironmentConfigSource}.
 */
class LegacyPortsEnvironmentConfigSourceTest {

    /** A {@link ServiceLoaderFunction} reporting a single {@link ConfigurationExtension} exposing {@link PortsConfig}. */
    private static ServiceLoaderFunction portsConfigServiceLoader() {
        return new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == ConfigurationExtension.class) {
                    final ConfigurationExtension extension = new ConfigurationExtension() {
                        @Override
                        public Set<Class<? extends Record>> getConfigDataTypes() {
                            return Set.of(PortsConfig.class);
                        }
                    };
                    return Stream.of(extension).map(e -> (C) e);
                }
                return Stream.empty();
            }
        };
    }

    /** No legacy env vars set: {@link PortsConfig} falls back to its own defaults. */
    @Test
    void noLegacyEnvVarsSet() {
        final ConfigSource source = LegacyPortsEnvironmentConfigSource.create(envName -> null);
        final Configuration config = ConfigurationBuilder.create()
                .withSource(source)
                .withConfigDataType(PortsConfig.class)
                .build();

        assertNull(config.getConfigData(PortsConfig.class).publisher());
        assertNull(config.getConfigData(PortsConfig.class).subscriber());
        assertNull(config.getConfigData(PortsConfig.class).blockAccess());
        assertNull(config.getConfigData(PortsConfig.class).serverStatus());
    }

    /** Each legacy env var, still set, feeds the matching renamed {@link PortsConfig} property. */
    @Test
    void legacyEnvVarsPopulatePortsConfig() {
        final ConfigSource source = LegacyPortsEnvironmentConfigSource.create(envName -> switch (envName) {
            case "BLOCK_ACCESS_PORT" -> "8081";
            case "SERVER_STATUS_PORT" -> "8082";
            case "PRODUCER_PORT" -> "8083";
            case "SUBSCRIBER_PORT" -> "8084";
            default -> null;
        });
        final Configuration config = ConfigurationBuilder.create()
                .withSource(source)
                .withConfigDataType(PortsConfig.class)
                .build();

        final PortsConfig portsConfig = config.getConfigData(PortsConfig.class);
        assertEquals(8083, portsConfig.publisher());
        assertEquals(8084, portsConfig.subscriber());
        assertEquals(8081, portsConfig.blockAccess());
        assertEquals(8082, portsConfig.serverStatus());
    }

    /** When both the legacy and the new-style env var are set for the same plugin, the new one wins. */
    @Test
    void newStyleEnvVarWinsOverLegacy() {
        final ConfigSource legacySource = LegacyPortsEnvironmentConfigSource.create(
                envName -> "BLOCK_ACCESS_PORT".equals(envName) ? "1111" : null);
        final AutomaticEnvironmentVariableConfigSource newStyleSource = new AutomaticEnvironmentVariableConfigSource(
                portsConfigServiceLoader(), envName -> "PORTS_BLOCK_ACCESS".equals(envName) ? "2222" : null);

        final Configuration config = ConfigurationBuilder.create()
                .withSource(legacySource)
                .withSource(newStyleSource)
                .withConfigDataType(PortsConfig.class)
                .build();

        assertEquals(2222, config.getConfigData(PortsConfig.class).blockAccess());
    }
}
