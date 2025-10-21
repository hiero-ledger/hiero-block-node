// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import org.hiero.block.tools.commands.days.subcommands.Compress;
import org.hiero.block.tools.commands.days.subcommands.DownloadDay;
import org.hiero.block.tools.commands.days.subcommands.DownloadDays;
import org.hiero.block.tools.commands.days.subcommands.Ls;
import org.hiero.block.tools.commands.days.subcommands.LsDayListing;
import org.hiero.block.tools.commands.days.subcommands.PrintListing;
import org.hiero.block.tools.commands.days.subcommands.Validate;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Top level command for working with record files, either .rcd or compressed .rcd.gz files.
 */
@Command(
        name = "records",
        description = "Tools for working with record files, .rcd or .rcd.gz",
        subcommands = {
            LsRecordFiles.class,
        },
        mixinStandardHelpOptions = true)
public class RecordsCommand implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // Use picocli to print the usage help (which includes subcommands) when no subcommand is specified
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
