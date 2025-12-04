// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days;

import org.hiero.block.tools.days.subcommands.CleanDayOfBadRecordSets;
import org.hiero.block.tools.days.subcommands.Compress;
import org.hiero.block.tools.days.subcommands.DownloadDay;
import org.hiero.block.tools.days.subcommands.DownloadDays;
import org.hiero.block.tools.days.subcommands.DownloadDaysV2;
import org.hiero.block.tools.days.subcommands.Ls;
import org.hiero.block.tools.days.subcommands.LsDayListing;
import org.hiero.block.tools.days.subcommands.PrintListing;
import org.hiero.block.tools.days.subcommands.SplitJsonToDayFiles;
import org.hiero.block.tools.days.subcommands.UpdateDayListingsCommand;
import org.hiero.block.tools.days.subcommands.Validate;
import org.hiero.block.tools.days.subcommands.ValidateSignatureCounts;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Top level command for working with compressed daily record file archives. These archives are tar.zstd files with a
 * directory per block which contains the record file, signature files and sidecar files. The important part is the
 * files in the archive are in ascending time order so you can read them start to finish chronologically.
 */
@Command(
        name = "days",
        description = "Works with compressed daily record file archives",
        subcommands = {
            Ls.class,
            Validate.class,
            ValidateSignatureCounts.class,
            Compress.class,
            DownloadDay.class,
            DownloadDaysV2.class,
            DownloadDays.class,
            PrintListing.class,
            LsDayListing.class,
            SplitJsonToDayFiles.class,
            CleanDayOfBadRecordSets.class,
            UpdateDayListingsCommand.class
        },
        mixinStandardHelpOptions = true)
public class DaysCommand implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // Use picocli to print the usage help (which includes subcommands) when no subcommand is specified
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
