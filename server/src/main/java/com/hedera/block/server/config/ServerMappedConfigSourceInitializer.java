// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.config;

import com.swirlds.config.extensions.sources.ConfigMapping;
import com.swirlds.config.extensions.sources.MappedConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * A class that extends {@link MappedConfigSource} ir order to have project-relevant initialization.
 */
public final class ServerMappedConfigSourceInitializer {
    static final List<ConfigMapping> MAPPINGS = List.of(
            // Please add properties in alphabetical order

            // Consumer Config
            new ConfigMapping(
                    "consumer.cueHistoricStreamingPaddingBlocks", "CONSUMER_CUE_HISTORIC_STREAMING_PADDING_BLOCKS"),
            new ConfigMapping("consumer.timeoutThresholdMillis", "CONSUMER_TIMEOUT_THRESHOLD_MILLIS"),
            new ConfigMapping("consumer.maxBlockItemBatchSize", "CONSUMER_MAX_BLOCK_ITEM_BATCH_SIZE"),

            // Mediator Config
            new ConfigMapping(
                    "mediator.historicTransitionThresholdPercentage",
                    "MEDIATOR_HISTORIC_TRANSITION_THRESHOLD_PERCENTAGE"),
            new ConfigMapping("mediator.ringBufferSize", "MEDIATOR_RING_BUFFER_SIZE"),
            new ConfigMapping("mediator.type", "MEDIATOR_TYPE"),

            // Notifier Config
            new ConfigMapping(
                    "notifier.historicTransitionThresholdPercentage",
                    "NOTIFIER_HISTORIC_TRANSITION_THRESHOLD_PERCENTAGE"),
            new ConfigMapping("notifier.ringBufferSize", "NOTIFIER_RING_BUFFER_SIZE"),

            // Persistence Config
            new ConfigMapping("persistence.storage.archiveRootPath", "PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH"),
            new ConfigMapping("persistence.storage.compression", "PERSISTENCE_STORAGE_COMPRESSION"),
            new ConfigMapping("persistence.storage.compressionLevel", "PERSISTENCE_STORAGE_COMPRESSION_LEVEL"),
            new ConfigMapping("persistence.storage.liveRootPath", "PERSISTENCE_STORAGE_LIVE_ROOT_PATH"),
            new ConfigMapping("persistence.storage.type", "PERSISTENCE_STORAGE_TYPE"),
            new ConfigMapping("persistence.storage.archiveGroupSize", "PERSISTENCE_STORAGE_ARCHIVE_GROUP_SIZE"),
            new ConfigMapping("persistence.storage.unverifiedRootPath", "PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH"),

            // Producer Config
            new ConfigMapping("producer.type", "PRODUCER_TYPE"),

            // Prometheus Config (externally managed, but we need this mapping)
            new ConfigMapping("prometheus.endpointEnabled", "PROMETHEUS_ENDPOINT_ENABLED"),
            new ConfigMapping("prometheus.endpointPortNumber", "PROMETHEUS_ENDPOINT_PORT_NUMBER"),

            // Server Config
            new ConfigMapping("server.maxMessageSizeBytes", "SERVER_MAX_MESSAGE_SIZE_BYTES"),
            new ConfigMapping("server.socketSendBufferSizeBytes", "SERVER_SOCKET_SEND_BUFFER_SIZE_BYTES"),
            new ConfigMapping("server.socketReceiveBufferSizeBytes", "SERVER_SOCKET_RECEIVE_BUFFER_SIZE_BYTES"),
            new ConfigMapping("server.port", "SERVER_PORT"),

            // Service Config
            new ConfigMapping("service.shutdownDelayMillis", "SERVICE_SHUTDOWN_DELAY_MILLIS"),

            // Verification Config
            new ConfigMapping("verification.hashCombineBatchSize", "VERIFICATION_HASH_COMBINE_BATCH_SIZE"),
            new ConfigMapping("verification.sessionType", "VERIFICATION_SESSION_TYPE"),
            new ConfigMapping("verification.type", "VERIFICATION_TYPE"));

    private ServerMappedConfigSourceInitializer() {}

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
