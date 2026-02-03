// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.metadata;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Top level command for working with metadata files. Contains subcommands for various operations.
 */
@Command(
        name = "metadata",
        description = "Works with metadata files in the \"metadata\" directory",
        subcommands = {
            MetadataLsCommand.class,
            MetadataUpdateCommand.class,
        },
        mixinStandardHelpOptions = true)
public class MetadataCommand implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // Use picocli to print the usage help (which includes subcommands) when no subcommand is specified
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
