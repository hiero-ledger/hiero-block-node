// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.util;

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
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.metrics.MetricsServiceImpl;

public class TestConfigUtil {
    public static final String CONSUMER_TIMEOUT_THRESHOLD_KEY = "consumer.timeoutThresholdMillis";
    public static final String MEDIATOR_RING_BUFFER_SIZE_KEY = "mediator.ringBufferSize";
    private static final String TEST_APP_PROPERTIES_FILE = "app.properties";

    private TestConfigUtil() {}

    @NonNull
    public static BlockNodeContext getTestBlockNodeContext(@NonNull Map<String, String> customProperties)
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

        Configuration testConfiguration = testConfigBuilder.build();

        Metrics metrics = getTestMetrics(testConfiguration);

        MetricsService metricsService = new MetricsServiceImpl(metrics);

        return new BlockNodeContext(metricsService, testConfiguration);
    }

    public static BlockNodeContext getTestBlockNodeContext() throws IOException {
        return getTestBlockNodeContext(Collections.emptyMap());
    }

    // TODO: we need to create a Mock metrics, and avoid using the real metrics for tests
    public static Metrics getTestMetrics(@NonNull Configuration configuration) {
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        return metrics;
    }
}
