// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

import java.time.LocalDate;

/**
 * Network-specific configuration for the Hedera block stream tools.
 *
 * <p>Provides all network-specific parameters needed by the CLI commands, such as
 * the GCS bucket name, mirror node URL, genesis date, node range, and total HBAR supply.
 * Factory methods are provided for known networks (mainnet, testnet). The static
 * {@link #current()} method returns the currently active configuration, which defaults
 * to mainnet and can be changed via the {@code --network} CLI option.
 *
 * @param networkName              human-readable network name (e.g. "mainnet", "testnet")
 * @param gcsBucketName            GCS bucket containing record streams
 * @param bucketPathPrefix         path prefix within the GCS bucket (e.g. "recordstreams/")
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
        String gcsBucketName,
        String bucketPathPrefix,
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
                "hedera-mainnet-streams",
                "recordstreams/",
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
                "hedera-testnet-streams-2024-02",
                "recordstreams/",
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
     * @param name the network name (case-insensitive): "mainnet" or "testnet"
     * @return the corresponding {@link NetworkConfig}
     * @throws IllegalArgumentException if the network name is not recognized
     */
    public static NetworkConfig fromName(final String name) {
        return switch (name.toLowerCase()) {
            case "mainnet" -> mainnet();
            case "testnet" -> testnet();
            default ->
                throw new IllegalArgumentException(
                        "Unknown network: " + name + ". Supported networks: mainnet, testnet");
        };
    }
}
