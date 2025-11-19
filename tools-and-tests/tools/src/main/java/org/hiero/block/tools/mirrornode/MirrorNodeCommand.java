// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Top level command for working with mirror nodes to fetch data.
 */
@Command(
        name = "mirror",
        description = "Works with mirror nodes to fetch data",
        subcommands = {
            ExtractBlockTimes.class,
            ValidateBlockTimes.class,
            AddNewerBlockTimes.class,
            FetchMirrorNodeRecordsCsv.class,
            ExtractDayBlocks.class,
            UpdateBlockData.class
        },
        mixinStandardHelpOptions = true)
public class MirrorNodeCommand implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // Use picocli to print the usage help (which includes subcommands) when no subcommand is specified
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
