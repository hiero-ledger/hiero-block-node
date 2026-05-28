// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

import java.time.LocalDate;

/**
 * Network-specific configuration for the Hedera block stream tools.
 *
 * <p>Provides all network-specific parameters needed by the CLI commands, such as
 * the bucket name, mirror node URL, genesis date, node range, and total HBAR supply.
 * Factory methods are provided for known networks (mainnet, testnet). The static
 * {@link #current()} method returns the currently active configuration, which defaults
 * to mainnet and can be changed via the {@code --network} CLI option.
 *
 * <p>Supports both GCS and S3-compatible storage (e.g., MinIO). The {@code bucketType}
 * field determines which storage backend is used:
 * <ul>
 *   <li>GCS: Uses {@code bucketName} directly with Google Cloud Storage API</li>
 *   <li>S3: Uses {@code bucketName} with S3-compatible storage (requires endpoint, region, accessKey, secretKey)</li>
 * </ul>
 *
 * @param networkName              human-readable network name (e.g. "mainnet", "testnet")
 * @param bucketType               type of cloud storage: GCS or S3
 * @param bucketName               bucket name containing record streams (GCS bucket name or S3 bucket name)
 * @param pathPrefix               path prefix within the bucket (e.g. "recordstreams/")
 * @param endpoint                 S3 endpoint URL (null for GCS)
 * @param region                   S3 region (null for GCS)
 * @param accessKey                S3 access key ID (null for GCS)
 * @param secretKey                S3 secret access key (null for GCS)
 * @param mirrorNodeApiUrl         base URL for the mirror node REST API (must end with '/')
 * @param genesisDate              first calendar day of the network
 * @param genesisTimestamp         first block timestamp in record-file format (underscores for colons)
 * @param minNodeAccountId         minimum consensus node account ID number
 * @param maxNodeAccountId         maximum consensus node account ID number
 * @param totalHbarSupplyTinybar   total HBAR supply in tinybar
 * @param genesisAddressBookResource classpath resource name for the genesis address book
 */
public record NetworkConfig(
        String networkName,
        BucketType bucketType,
        String bucketName,
        String pathPrefix,
        String endpoint,
        String region,
        String accessKey,
        String secretKey,
        String mirrorNodeApiUrl,
        LocalDate genesisDate,
        String genesisTimestamp,
        int minNodeAccountId,
        int maxNodeAccountId,
        long totalHbarSupplyTinybar,
        String genesisAddressBookResource) {

    /** The currently active network configuration. Defaults to mainnet. */
    private static volatile NetworkConfig current = mainnet();

    /**
     * Returns the currently active network configuration.
     *
     * @return the current {@link NetworkConfig}
     */
    public static NetworkConfig current() {
        return current;
    }

    /**
     * Sets the currently active network configuration.
     *
     * @param config the {@link NetworkConfig} to use
     */
    public static void setCurrent(final NetworkConfig config) {
        current = config;
    }

    /**
     * Returns the mainnet configuration.
     *
     * @return a {@link NetworkConfig} for Hedera mainnet
     */
    public static NetworkConfig mainnet() {
        return new NetworkConfig(
                "mainnet",
                BucketType.GCS,
                "hedera-mainnet-streams",
                "recordstreams/",
                null, // endpoint (GCS)
                null, // region (GCS)
                null, // accessKey (GCS)
                null, // secretKey (GCS)
                "https://mainnet-public.mirrornode.hedera.com/api/v1/",
                LocalDate.of(2019, 9, 13),
                "2019-09-13T21_53_51.396440Z",
                3,
                37,
                5_000_000_000_000_000_000L,
                "mainnet-genesis-address-book.proto.bin");
    }

    /**
     * Returns the testnet configuration.
     *
     * <p>Based on the current testnet instance (reset February 2024). Testnet has 7 consensus
     * nodes with account IDs 0.0.3 through 0.0.9.
     *
     * @return a {@link NetworkConfig} for Hedera testnet
     */
    public static NetworkConfig testnet() {
        return new NetworkConfig(
                "testnet",
                BucketType.GCS,
                "hedera-testnet-streams-2024-02",
                "recordstreams/",
                null, // endpoint (GCS)
                null, // region (GCS)
                null, // accessKey (GCS)
                null, // secretKey (GCS)
                "https://testnet.mirrornode.hedera.com/api/v1/",
                LocalDate.of(2024, 2, 1),
                "2024-02-01T18_35_20.644859297Z",
                3,
                9,
                5_000_000_000_000_000_000L,
                "testnet-genesis-address-book.proto.bin");
    }

    /**
     * Creates a {@link NetworkConfig} from a network name.
     *
     * <p>Supported networks:
     * <ul>
     *   <li>{@code mainnet} - Hedera mainnet (hardcoded configuration)</li>
     *   <li>{@code testnet} - Hedera testnet (hardcoded configuration)</li>
     *   <li>{@code previewnet} - Hedera previewnet (loaded from ~/.hiero/networks/previewnet-config.json)</li>
     *   <li>{@code other} - Custom network (loaded from path specified in HIERO_NETWORK_CONFIG env var)</li>
     * </ul>
     *
     * @param name the network name (case-insensitive): "mainnet", "testnet", "previewnet", or "other"
     * @return the corresponding {@link NetworkConfig}
     * @throws IllegalArgumentException if the network name is not recognized or config cannot be loaded
     */
    public static NetworkConfig fromName(final String name) {
        return switch (name.toLowerCase()) {
            case "mainnet" -> mainnet();
            case "testnet" -> testnet();
            case "previewnet" -> NetworkConfigLoader.loadNetworkConfig("previewnet");
            case "other" -> NetworkConfigLoader.loadCustomNetworkConfig();
            default ->
                throw new IllegalArgumentException(
                        "Unknown network: " + name + ". Supported networks: mainnet, testnet, previewnet, other");
        };
    }
}
