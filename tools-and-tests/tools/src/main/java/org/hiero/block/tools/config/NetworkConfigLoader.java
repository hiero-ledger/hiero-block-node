// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Loads network configurations from JSON files.
 *
 * <p>Supports loading network configs for previewnet and custom networks from JSON files
 * with the following format:
 * <pre>{@code
 * {
 *   "networkName": "previewnet",
 *   "gcsBucketName": "hedera-preview-streams",
 *   "bucketPathPrefix": "recordstreams/",
 *   "mirrorNodeApiUrl": "https://previewnet.mirrornode.hedera.com/api/v1/",
 *   "genesisDate": "2023-04-06",
 *   "genesisTimestamp": "2023-04-06T12_00_00.000000000Z",
 *   "minNodeAccountId": 3,
 *   "maxNodeAccountId": 9,
 *   "totalHbarSupplyTinybar": 5000000000000000000,
 *   "genesisAddressBookResource": "previewnet-genesis-address-book.proto.bin"
 * }
 * }</pre>
 */
public final class NetworkConfigLoader {

    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(LocalDate.class, new LocalDateDeserializer())
            .create();

    private NetworkConfigLoader() {
        // Utility class
    }

    /**
     * Loads a network configuration from a well-known location.
     *
     * <p>Looks for config file at: {@code ~/.hiero/networks/{networkName}-config.json}
     *
     * @param networkName the network name (e.g., "previewnet")
     * @return the loaded {@link NetworkConfig}
     * @throws IllegalArgumentException if the config file is not found or invalid
     */
    public static NetworkConfig loadNetworkConfig(final String networkName) {
        final Path configPath = getDefaultConfigPath(networkName);
        return loadFromPath(configPath);
    }

    /**
     * Loads a custom network configuration from a specified path.
     *
     * <p>The path is read from the {@code HIERO_NETWORK_CONFIG} environment variable.
     *
     * @return the loaded {@link NetworkConfig}
     * @throws IllegalArgumentException if the environment variable is not set or the config is invalid
     */
    public static NetworkConfig loadCustomNetworkConfig() {
        final String configPathStr = System.getenv("HIERO_NETWORK_CONFIG");
        if (configPathStr == null || configPathStr.isBlank()) {
            throw new IllegalArgumentException(
                    "Custom network requires HIERO_NETWORK_CONFIG environment variable pointing to config file");
        }
        final Path configPath = Path.of(configPathStr);
        return loadFromPath(configPath);
    }

    /**
     * Loads a network configuration from a specified path.
     *
     * @param configPath the path to the JSON config file
     * @return the loaded {@link NetworkConfig}
     * @throws IllegalArgumentException if the file is not found or invalid
     */
    public static NetworkConfig loadFromPath(final Path configPath) {
        if (!Files.exists(configPath)) {
            throw new IllegalArgumentException("Network config file not found: " + configPath);
        }

        try {
            final String json = Files.readString(configPath);
            final NetworkConfig config = GSON.fromJson(json, NetworkConfig.class);

            if (config == null) {
                throw new IllegalArgumentException("Failed to parse network config: " + configPath);
            }

            validateConfig(config);
            return config;

        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read network config file: " + configPath, e);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("Invalid JSON in network config file: " + configPath, e);
        }
    }

    /**
     * Returns the default config file path for a network.
     *
     * @param networkName the network name
     * @return the path: {@code ~/.hiero/networks/{networkName}-config.json}
     */
    private static Path getDefaultConfigPath(final String networkName) {
        final String userHome = System.getProperty("user.home");
        return Path.of(userHome, ".hiero", "networks", networkName + "-config.json");
    }

    /**
     * Validates that all required fields are present in the config.
     *
     * @param config the config to validate
     * @throws IllegalArgumentException if any required field is missing or invalid
     */
    private static void validateConfig(final NetworkConfig config) {
        if (config.networkName() == null || config.networkName().isBlank()) {
            throw new IllegalArgumentException("Network config missing required field: networkName");
        }
        if (config.gcsBucketName() == null || config.gcsBucketName().isBlank()) {
            throw new IllegalArgumentException("Network config missing required field: gcsBucketName");
        }
        if (config.mirrorNodeApiUrl() == null || config.mirrorNodeApiUrl().isBlank()) {
            throw new IllegalArgumentException("Network config missing required field: mirrorNodeApiUrl");
        }
        if (config.genesisDate() == null) {
            throw new IllegalArgumentException("Network config missing required field: genesisDate");
        }
        if (config.genesisTimestamp() == null || config.genesisTimestamp().isBlank()) {
            throw new IllegalArgumentException("Network config missing required field: genesisTimestamp");
        }
    }

    /**
     * Custom deserializer for LocalDate from ISO-8601 format (YYYY-MM-DD).
     */
    private static class LocalDateDeserializer implements JsonDeserializer<LocalDate> {
        @Override
        public LocalDate deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            return LocalDate.parse(json.getAsString(), DateTimeFormatter.ISO_LOCAL_DATE);
        }
    }
}
