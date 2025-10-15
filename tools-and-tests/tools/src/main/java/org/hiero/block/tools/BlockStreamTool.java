// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools;

import org.hiero.block.tools.commands.ConvertToJson;
import org.hiero.block.tools.commands.Info;
import org.hiero.block.tools.commands.days.DaysCommand;
import org.hiero.block.tools.commands.record2blocks.Record2BlockCommand;
import org.hiero.block.tools.commands.record2blocks.gcp.AddNewerBlockTimes;
import org.hiero.block.tools.commands.record2blocks.mirrornode.ExtractBlockTimes;
import org.hiero.block.tools.commands.record2blocks.mirrornode.FetchMirrorNodeRecordsCsv;
import org.hiero.block.tools.commands.record2blocks.mirrornode.ValidateBlockTimes;
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
            ConvertToJson.class,
            Info.class,
            Record2BlockCommand.class,
            FetchMirrorNodeRecordsCsv.class,
            ExtractBlockTimes.class,
            ValidateBlockTimes.class,
            AddNewerBlockTimes.class,
            DaysCommand.class
        })
public final class BlockStreamTool {

    /**
     * Empty Default constructor to remove JavaDoc warning
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
