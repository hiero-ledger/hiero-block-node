// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools;

import org.hiero.block.tools.blocks.BlocksCommand;
import org.hiero.block.tools.days.DaysCommand;
import org.hiero.block.tools.metadata.MetadataCommand;
import org.hiero.block.tools.mirrornode.MirrorNodeCommand;
import org.hiero.block.tools.commands.ConvertToJson;
import org.hiero.block.tools.commands.Info;
import org.hiero.block.tools.commands.NetworkCapacity;
import org.hiero.block.tools.commands.days.DaysCommand;
import org.hiero.block.tools.commands.mirrornode.MirrorNodeCommand;
import org.hiero.block.tools.records.RecordsCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Command line tool for working with Hedera block stream files
 */
@SuppressWarnings("InstantiationOfUtilityClass")
@Command(
        name = "subcommands",
        mixinStandardHelpOptions = true,
        version = "BlockStreamTool 0.1",
        subcommands = {
            BlocksCommand.class,
            RecordsCommand.class,
            ConvertToJson.class,
            Info.class,
            Record2BlockCommand.class,
            FetchMirrorNodeRecordsCsv.class,
            ExtractBlockTimes.class,
            ValidateBlockTimes.class,
            AddNewerBlockTimes.class,
            NetworkCapacity.class,
            DaysCommand.class,
            MirrorNodeCommand.class,
            MetadataCommand.class,
        })
public final class BlockStreamTool {

    /**
     * Empty Default constructor to remove Javadoc warning
     */
    public BlockStreamTool() {}

    /**
     * Main entry point for the app
     * @param args command line arguments
     */
    public static void main(String... args) {
        int exitCode = new CommandLine(new BlockStreamTool()).execute(args);
        System.exit(exitCode);
    }
}
