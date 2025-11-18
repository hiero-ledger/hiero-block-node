// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Top level command for working with block stream files. Contains subcommands for various operations.
 */
@Command(
        name = "blocks",
        description = "Works with block stream files",
        subcommands = {
            ConvertToJson.class,
            Info.class,
            ValidateBlocksCommand.class,
        },
        mixinStandardHelpOptions = true)
public class BlocksCommand implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // Use picocli to print the usage help (which includes subcommands) when no subcommand is specified
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
