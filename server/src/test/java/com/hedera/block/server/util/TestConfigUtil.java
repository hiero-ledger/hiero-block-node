// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.util;

import com.hedera.block.server.consumer.ConsumerConfig;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.metrics.MetricsServiceImpl;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public class TestConfigUtil {
    public static final String CONSUMER_TIMEOUT_THRESHOLD_KEY = "consumer.timeoutThresholdMillis";
    public static final String MEDIATOR_RING_BUFFER_SIZE_KEY = "mediator.ringBufferSize";
    private static final String TEST_APP_PROPERTIES_FILE = "app.properties";

    private TestConfigUtil() {}

    @NonNull
    public static Configuration getTestBlockNodeConfiguration(@NonNull Map<String, String> customProperties)
            throws IOException {

        // create test configuration
        ConfigurationBuilder testConfigBuilder = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withSource(new ClasspathFileConfigSource(Path.of(TEST_APP_PROPERTIES_FILE)));

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            testConfigBuilder = testConfigBuilder.withValue(key, value);
        }

        testConfigBuilder = testConfigBuilder.withConfigDataType(ConsumerConfig.class);
        return testConfigBuilder.build();
    }

    @NonNull
    public static MetricsService getTestBlockNodeMetricsService(@NonNull Map<String, String> customProperties)
            throws IOException {

        Metrics metrics = getTestMetrics(getTestBlockNodeConfiguration(customProperties));
        return new MetricsServiceImpl(metrics);
    }

    @NonNull
    public static MetricsService getTestBlockNodeMetricsService(@NonNull Configuration configuration)
            throws IOException {

        Metrics metrics = getTestMetrics(configuration);
        return new MetricsServiceImpl(metrics);
    }

    @NonNull
    public static Configuration getTestBlockNodeConfiguration() throws IOException {
        return getTestBlockNodeConfiguration(Collections.emptyMap());
    }

    @NonNull
    public static MetricsService getTestBlockNodeMetricsService() throws IOException {
        Metrics metrics = getTestMetrics(getTestBlockNodeConfiguration(Collections.emptyMap()));
        return new MetricsServiceImpl(metrics);
    }

    // TODO: we need to create a Mock metrics, and avoid using the real metrics for tests
    public static Metrics getTestMetrics(@NonNull Configuration configuration) {
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        return metrics;
    }
}
