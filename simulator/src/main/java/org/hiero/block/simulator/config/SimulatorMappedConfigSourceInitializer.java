// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config;

import com.swirlds.config.extensions.sources.ConfigMapping;
import com.swirlds.config.extensions.sources.MappedConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * A class that extends {@link MappedConfigSource} in order to have project-relevant initialization.
 */
public final class SimulatorMappedConfigSourceInitializer {
    private static final List<ConfigMapping> MAPPINGS = List.of(
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
            new ConfigMapping("blockStream.midBlockFailType", "BLOCK_STREAM_MID_BLOCK_FAIL_TYPE"),
            new ConfigMapping("blockStream.midBlockFailOffset", "BLOCK_STREAM_MID_BLOCK_FAIL_OFFSET"),

            // Block consumer configuration
            new ConfigMapping("consumer.startBlockNumber", "CONSUMER_START_BLOCK_NUMBER"),
            new ConfigMapping("consumer.endBlockNumber", "CONSUMER_END_BLOCK_NUMBER"),
            new ConfigMapping("consumer.slowDown", "CONSUMER_SLOWDOWN"),
            new ConfigMapping("consumer.randomSlowDown", "CONSUMER_RANDOM_SLOWDOWN"),
            new ConfigMapping("consumer.slowDownMilliseconds", "CONSUMER_SLOWDOWN_MILLISECONDS"),
            new ConfigMapping("consumer.slowDownForBlockRange", "CONSUMER_SLOWDOWN_FOR_BLOCK_RANGE"),

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
            new ConfigMapping("generator.invalidBlockHash", "GENERATOR_INVALID_BLOCK_HASH"),

            // Prometheus configuration
            new ConfigMapping("prometheus.endpointEnabled", "PROMETHEUS_ENDPOINT_ENABLED"),
            new ConfigMapping("prometheus.endpointPortNumber", "PROMETHEUS_ENDPOINT_PORT_NUMBER"),

            // Startup Data Config
            new ConfigMapping("simulator.startup.data.enabled", "SIMULATOR_STARTUP_DATA_ENABLED"),
            new ConfigMapping(
                    "simulator.startup.data.latestAckBlockNumberPath",
                    "SIMULATOR_STARTUP_DATA_LATEST_ACK_BLOCK_NUMBER_PATH"),
            new ConfigMapping(
                    "simulator.startup.data.latestAckBlockHashPath",
                    "SIMULATOR_STARTUP_DATA_LATEST_ACK_BLOCK_HASH_PATH"),

            // Block stream configuration
            new ConfigMapping("unorderedStream.enabled", "UNORDERED_STREAM_ENABLED"),
            new ConfigMapping("unorderedStream.availableBlocks", "UNORDERED_STREAM_AVAILABLE_BLOCKS"),
            new ConfigMapping("unorderedStream.sequenceScrambleLevel", "UNORDERED_STREAM_SEQUENCE_SCRAMBLE_LEVEL"),
            new ConfigMapping("unorderedStream.fixedStreamingSequence", "UNORDERED_STREAM_FIXED_STREAMING_SEQUENCE"));

    private SimulatorMappedConfigSourceInitializer() {}

    /**
     * This method constructs, initializes and returns a new instance of {@link MappedConfigSource}
     * which internally uses {@link SystemEnvironmentConfigSource} and maps relevant config keys to
     * other keys so that they could be used within the application
     *
     * @return newly constructed fully initialized {@link MappedConfigSource}
     */
    @NonNull
    public static MappedConfigSource getMappedConfigSource() {
        final MappedConfigSource config = new MappedConfigSource(SystemEnvironmentConfigSource.getInstance());
        MAPPINGS.forEach(config::addMapping);
        return config;
    }
}
