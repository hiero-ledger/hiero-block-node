// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days;

import org.hiero.block.tools.commands.days.subcommands.Compress;
import org.hiero.block.tools.commands.days.subcommands.DownloadDay;
import org.hiero.block.tools.commands.days.subcommands.DownloadDays;
import org.hiero.block.tools.commands.days.subcommands.Ls;
import org.hiero.block.tools.commands.days.subcommands.PrintListing;
import org.hiero.block.tools.commands.days.subcommands.Validate;
import picocli.CommandLine.Command;

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
            Compress.class,
            DownloadDay.class,
            DownloadDays.class,
            PrintListing.class
        },
        mixinStandardHelpOptions = true)
public class DaysCommand implements Runnable {
    @Override
    public void run() {
        System.out.println(
                "Please specify a subcommand: ls | validate | compress | download-day | download-days | print-listing\nUse --help for more details.");
    }
}
