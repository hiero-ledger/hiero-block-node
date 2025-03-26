// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import org.junit.jupiter.api.Test;

public class ConfigurationLoggingImplTest {

    @Test
    public void testCurrentAppProperties() { // throws IOException {

        //        final Configuration configuration = getTestConfig(Collections.emptyMap());
        //        final ConfigurationLogging configurationLogging = new ConfigurationLogging(configuration);
        //        final Map<String, Object> config = configurationLogging.collectConfig(configuration);
        //        assertNotNull(config);
        //        assertEquals(40, config.size());
        //
        //        for (Map.Entry<String, Object> entry : config.entrySet()) {
        //            String value = entry.getValue().toString();
        //            if (value.contains("*")) {
        //                fail(
        //                        "Current configuration not expected to contain any sensitive data. Change this test if
        // we add sensitive data.");
        //            }
        //        }
    }
    //
    //    @Test
    //    public void testWithMockedSensitiveProperty() throws IOException {
    //        final Configuration configuration = getTestConfigWithSecret();
    //        final ConfigurationLogging configurationLogging = new ConfigurationLogging(configuration);
    //        final Map<String, Object> config = configurationLogging.collectConfig(configuration);
    //        assertNotNull(config);
    //        assertEquals(42, config.size());
    //
    //        assertEquals("*****", config.get("test.secret").toString());
    //        assertEquals("", config.get("test.emptySecret").toString());
    //    }
    //
    //    @Test
    //    public void testMaxLineLength() {
    //        final Map<String, Object> testMap = Map.of("key1", "valueLongerString", "key2", "value2", "key3",
    // "value3");
    //        int length = ConfigurationLogging.calculateMaxLineLength(testMap);
    //
    //        assertEquals(21, length);
    //    }

    //    private static Configuration getTestConfig(@NonNull Map<String, String> customProperties) throws IOException {
    //
    //        // create test configuration
    //        ConfigurationBuilder testConfigBuilder = ConfigurationBuilder.create()
    //                .autoDiscoverExtensions()
    //                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")));
    //
    //        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
    //            String key = entry.getKey();
    //            String value = entry.getValue();
    //            testConfigBuilder = testConfigBuilder.withValue(key, value);
    //        }
    //
    //        testConfigBuilder = testConfigBuilder.withConfigDataType(ConsumerConfig.class);
    //        return testConfigBuilder.build();
    //    }
    //
    //    private static Configuration getTestConfigWithSecret() throws IOException {
    //
    //        ConfigurationBuilder testConfigBuilder = ConfigurationBuilder.create()
    //                .autoDiscoverExtensions()
    //                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")));
    //        testConfigBuilder = testConfigBuilder.withConfigDataType(TestSecretConfig.class);
    //        return testConfigBuilder.build();
    //    }
}
