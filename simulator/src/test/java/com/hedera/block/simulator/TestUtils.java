// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator;

import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;

public class TestUtils {

    private static final String TEST_APP_PROPERTIES_FILE = "app.properties";

    public static Configuration getTestConfiguration(@NonNull Map<String, String> customProperties) throws IOException {

        ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withSource(new ClasspathFileConfigSource(Path.of(TEST_APP_PROPERTIES_FILE)));

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            configurationBuilder.withSource(new SimpleConfigSource(key, value).withOrdinal(500));
        }

        return configurationBuilder.build();
    }

    public static Configuration getTestConfiguration() throws IOException {
        return getTestConfiguration(Map.of());
    }

    public static Metrics getTestMetrics(@NonNull Configuration configuration) {
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        return metrics;
    }

    public static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
