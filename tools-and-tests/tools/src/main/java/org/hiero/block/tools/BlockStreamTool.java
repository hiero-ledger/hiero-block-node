// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools;

import org.hiero.block.tools.blocks.BlocksCommand;
import org.hiero.block.tools.commands.NetworkCapacity;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.days.DaysCommand;
import org.hiero.block.tools.metadata.MetadataCommand;
import org.hiero.block.tools.mirrornode.MirrorNodeCommand;
import org.hiero.block.tools.records.RecordsCommand;
import org.hiero.block.tools.states.StatesCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * Command line tool for working with Hedera block stream files
 */
@Command(
        name = "subcommands",
        mixinStandardHelpOptions = true,
        version = "BlockStreamTool 0.1",
        subcommands = {
            BlocksCommand.class,
            RecordsCommand.class,
            DaysCommand.class,
            MirrorNodeCommand.class,
            MetadataCommand.class,
            NetworkCapacity.class,
            StatesCommand.class,
        })
public final class BlockStreamTool {

    /**
     * Empty Default constructor to remove Javadoc warning
     */
    public BlockStreamTool() {}

    /**
     * Sets the active network configuration. This option is inherited by all subcommands
     * via {@code scope = ScopeType.INHERIT}, so it can be specified at any level of the
     * command hierarchy (e.g., {@code tools days download-days-v3 --network testnet}).
     *
     * @param network the network name: "mainnet" (default) or "testnet"
     */
    @Option(
            names = {"-n", "--network"},
            description = "Network to use: mainnet (default), testnet",
            defaultValue = "mainnet",
            scope = ScopeType.INHERIT)
    void setNetwork(final String network) {
        NetworkConfig.setCurrent(NetworkConfig.fromName(network));
    }

    /**
     * Main entry point for the app
     * @param args command line arguments
     */
    static void main(String... args) {
        int exitCode = new CommandLine(new BlockStreamTool()).execute(args);
        System.exit(exitCode);
    }
}
