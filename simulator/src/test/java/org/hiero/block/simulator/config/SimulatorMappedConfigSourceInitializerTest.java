// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.extensions.sources.ConfigMapping;
import com.swirlds.config.extensions.sources.MappedConfigSource;
import java.lang.System.Logger.Level;
import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.hiero.block.simulator.config.logging.Loggable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SimulatorMappedConfigSourceInitializerTest {
    private static final System.Logger LOGGER =
            System.getLogger(SimulatorMappedConfigSourceInitializerTest.class.getName());
    private static final Set<String> LOGGABLE_PACKAGES = Set.of("metrics", "prometheus");
    private static final ConfigMapping[] SUPPORTED_MAPPINGS = {
        // gRPC configuration
        new ConfigMapping("grpc.serverAddress", "GRPC_SERVER_ADDRESS"),
        new ConfigMapping("grpc.port", "GRPC_PORT"),

        // Block stream configuration
        new ConfigMapping("blockStream.simulatorMode", "BLOCK_STREAM_SIMULATOR_MODE"),
        new ConfigMapping("blockStream.lastKnownStatusesCapacity", "BLOCK_STREAM_LAST_KNOWN_STATUSES_CAPACITY"),
        new ConfigMapping("blockStream.delayBetweenBlockItems", "BLOCK_STREAM_DELAY_BETWEEN_BLOCK_ITEMS"),
        new ConfigMapping("blockStream.maxBlockItemsToStream", "BLOCK_STREAM_MAX_BLOCK_ITEMS_TO_STREAM"),
        new ConfigMapping("blockStream.streamingMode", "BLOCK_STREAM_STREAMING_MODE"),
        new ConfigMapping("blockStream.millisecondsPerBlock", "BLOCK_STREAM_MILLISECONDS_PER_BLOCK"),
        new ConfigMapping("blockStream.blockItemsBatchSize", "BLOCK_STREAM_BLOCK_ITEMS_BATCH_SIZE"),

        // Block consumer configuration
        new ConfigMapping("consumer.startBlockNumber", "CONSUMER_START_BLOCK_NUMBER"),
        new ConfigMapping("consumer.endBlockNumber", "CONSUMER_END_BLOCK_NUMBER"),

        // Block generator configuration
        new ConfigMapping("generator.generationMode", "GENERATOR_GENERATION_MODE"),
        new ConfigMapping("generator.minEventsPerBlock", "GENERATOR_MIN_EVENTS_PER_BLOCK"),
        new ConfigMapping("generator.maxEventsPerBlock", "GENERATOR_MAX_EVENTS_PER_BLOCK"),
        new ConfigMapping("generator.minTransactionsPerEvent", "GENERATOR_MIN_TRANSACTIONS_PER_EVENT"),
        new ConfigMapping("generator.maxTransactionsPerEvent", "GENERATOR_MAX_TRANSACTIONS_PER_EVENT"),
        new ConfigMapping("generator.folderRootPath", "GENERATOR_FOLDER_ROOT_PATH"),
        new ConfigMapping("generator.managerImplementation", "GENERATOR_MANAGER_IMPLEMENTATION"),
        new ConfigMapping("generator.paddedLength", "GENERATOR_PADDED_LENGTH"),
        new ConfigMapping("generator.fileExtension", "GENERATOR_FILE_EXTENSION"),
        new ConfigMapping("generator.startBlockNumber", "GENERATOR_START_BLOCK_NUMBER"),
        new ConfigMapping("generator.endBlockNumber", "GENERATOR_END_BLOCK_NUMBER"),

        // Prometheus configuration (externally managed, but we need this mapping)
        new ConfigMapping("prometheus.endpointEnabled", "PROMETHEUS_ENDPOINT_ENABLED"),
        new ConfigMapping("prometheus.endpointPortNumber", "PROMETHEUS_ENDPOINT_PORT_NUMBER")
    };

    /**
     * This test aims to verify the state of all config extensions we have
     * added. These configs are the ones that are returned from
     * {@link SimulatorConfigExtension#getConfigDataTypes()}. This test will
     * verify:
     * <pre>
     *     - all added config classes are annotated with the {@link ConfigData}
     *       annotation.
     *     - all fields in all config classes are annotated with the
     *       {@link ConfigProperty} annotation.
     *     - a mapping for all fields in all config classes is present in the
     *       {@link SimulatorMappedConfigSourceInitializer#MAPPINGS()}, as
     *       defined in {@link #allManagedConfigDataTypes()}.
     * </pre>
     * @param config parameterized, config class to test
     * @param fieldNamesToExclude parameterized, fields to exclude from the test
     */
    @ParameterizedTest
    @MethodSource("allManagedConfigDataTypes")
    void testVerifyAllFieldsInRecordsAreMapped(
            final Class<? extends Record> config, final List<String> fieldNamesToExclude) {
        final String configClassName = config.getSimpleName();
        if (!config.isAnnotationPresent(ConfigData.class)) {
            Assertions.fail(
                    "Class [%s] is missing the ConfigData annotation! All config classes MUST have that annotation present!"
                            .formatted(configClassName));
        } else {
            final ConfigData configDataAnnotation = config.getDeclaredAnnotation(ConfigData.class);
            final String prefix = configDataAnnotation.value();
            final RecordComponent[] fieldsToVerify = Arrays.stream(config.getRecordComponents())
                    .filter(recordComponent -> !fieldNamesToExclude.contains(recordComponent.getName()))
                    .toArray(RecordComponent[]::new);
            LOGGER.log(
                    Level.INFO,
                    "Checking fields %s\nfor config class [%s]."
                            .formatted(Arrays.toString(fieldsToVerify), configClassName));
            for (final RecordComponent recordComponent : fieldsToVerify) {
                final String fieldName = recordComponent.getName();
                verifyConfigPropertyAnnotation(recordComponent, fieldName, configClassName, prefix);
                verifyLoggableAnnotation(recordComponent, fieldName, configClassName, prefix);
            }
        }
    }

    private void verifyConfigPropertyAnnotation(
            final RecordComponent recordComponent,
            final String fieldName,
            final String configClassName,
            final String prefix) {
        if (!recordComponent.isAnnotationPresent(ConfigProperty.class)) {
            fail(
                    "Field [%s] in [%s] is missing the ConfigProperty annotation! All fields in config classes MUST have that annotation present!"
                            .formatted(fieldName, configClassName));
        } else {
            final String expectedMappedName = "%s.%s".formatted(prefix, fieldName);
            final Optional<ConfigMapping> matchingMapping = Arrays.stream(SUPPORTED_MAPPINGS)
                    .filter(mapping -> mapping.mappedName().equals(expectedMappedName))
                    .findFirst();
            assertThat(matchingMapping)
                    .isNotNull()
                    .withFailMessage(
                            "Field [%s] in [%s] is not present in the environment variable mappings! Expected config key [%s] to be present and to be mapped to [%s]",
                            fieldName,
                            configClassName,
                            expectedMappedName,
                            transformToEnvVarConvention(expectedMappedName))
                    .isPresent();
        }
    }

    private void verifyLoggableAnnotation(
            final RecordComponent recordComponent,
            final String fieldName,
            final String configClassName,
            final String prefix) {
        if (!recordComponent.isAnnotationPresent(Loggable.class) && !LOGGABLE_PACKAGES.contains(prefix)) {
            fail(
                    "Field [%s] in [%s] is missing the Loggable annotation! All fields in config classes MUST have that annotation present!"
                            .formatted(fieldName, configClassName));
        }
    }

    /**
     * This test aims to fail if we have added or removed any {@link ConfigMapping} that will be
     * initialized by the {@link SimulatorMappedConfigSourceInitializer#getMappedConfigSource()}
     * without reflecting it here in the test. The purpose is to bring attention to any changes to
     * the developer so we can make sure we are aware of them in order to be sure we require the
     * change. This test is more to bring attention than to test actual logic. So if this fails, we
     * either change the {@link #SUPPORTED_MAPPINGS} here or the {@link
     * SimulatorMappedConfigSourceInitializer#MAPPINGS} to make this pass.
     */
    @Test
    void testVerifyAllSupportedMappingsAreAddedToInstance() throws ReflectiveOperationException {
        final Queue<ConfigMapping> actual = extractConfigMappings();

        // fail if the actual and this test have a different number of mappings
        assertThat(SUPPORTED_MAPPINGS.length)
                .withFailMessage(
                        "The number of supported mappings has changed! Please update the test to reflect the change.\nRUNTIME_MAPPING: %s\nTEST_MAPPING: %s",
                        actual, Arrays.toString(SUPPORTED_MAPPINGS))
                .isEqualTo(actual.size());

        // test this test against actual
        for (final ConfigMapping current : SUPPORTED_MAPPINGS) {
            final Predicate<ConfigMapping> predicate =
                    cm -> current.mappedName().equals(cm.mappedName())
                            && current.originalName().equals(cm.originalName());
            assertThat(actual.stream().anyMatch(predicate))
                    .withFailMessage(
                            "When testing for: [%s] it is not contained in mappings of the actual initialized object %s",
                            current, actual)
                    .isTrue();
        }

        // test actual against this test
        for (final ConfigMapping current : actual) {
            final Predicate<ConfigMapping> predicate =
                    cm -> current.mappedName().equals(cm.mappedName())
                            && current.originalName().equals(cm.originalName());
            assertThat(Arrays.stream(SUPPORTED_MAPPINGS).anyMatch(predicate))
                    .withFailMessage(
                            "When testing for: [%s] it is not contained in mappings of this test %s", current, actual)
                    .isTrue();
        }
    }

    @SuppressWarnings("unchecked")
    private static Queue<ConfigMapping> extractConfigMappings() throws ReflectiveOperationException {
        final Field configMappings = MappedConfigSource.class.getDeclaredField("configMappings");
        try {
            configMappings.setAccessible(true);
            return (Queue<ConfigMapping>)
                    configMappings.get(SimulatorMappedConfigSourceInitializer.getMappedConfigSource());
        } finally {
            configMappings.setAccessible(false);
        }
    }

    private static String transformToEnvVarConvention(final String input) {
        final String underscored = input.replace(".", "_");
        final String resolved = underscored.replaceAll("(?<!_)([A-Z])", "_$1");
        return resolved.toUpperCase();
    }

    private static Stream<Arguments> allManagedConfigDataTypes() {
        // Add any classes that should be excluded from the test for any reason in the set below
        // MetricsConfig is not managed by us.
        final Set<Class<? extends Record>> excluded = Set.of(MetricsConfig.class);

        // Add any classes that should be partially checked in the map below
        // for example, we do not manage PrometheusConfig, but we need the endpointEnabled and endpointPortNumber
        // mappings to be present in our scope, so we exclude all other fields. We must do the strategy
        // of exclusion and not inclusion because fields in the config classes can be added or removed, we want
        // to detect that.
        final Entry<Class<PrometheusConfig>, List<String>> prometheusFieldsToExclude =
                Map.entry(PrometheusConfig.class, List.of("endpointMaxBacklogAllowed"));
        final Map<Class<? extends Record>, List<String>> fieldNamesToExcludeForConfig =
                Map.ofEntries(prometheusFieldsToExclude);

        // Here are all the config classes that we need to check mappings for
        final Set<Class<? extends Record>> allRegisteredRecordsFilteredWithExcluded = new SimulatorConfigExtension()
                .getConfigDataTypes().stream()
                        .filter(configType -> !excluded.contains(configType))
                        .collect(Collectors.toSet());

        // Here we return all config classes that we need to check mappings for and a list of field names to
        // exclude from the test for the given config class. If the list is empty, all fields will be checked.
        return allRegisteredRecordsFilteredWithExcluded.stream().map(config -> {
            final List<String> fieldsToExclude = fieldNamesToExcludeForConfig.getOrDefault(config, List.of());
            return Arguments.of(config, fieldsToExclude);
        });
    }
}
