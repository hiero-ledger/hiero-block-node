// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import org.hiero.block.tools.blocks.wrapped.FetchBalanceCheckpointsCommand;
import org.hiero.block.tools.blocks.wrapped.ValidateWrappedBlocksCommand;
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
            LsBlockFiles.class,
            ValidateBlocksCommand.class,
            ToWrappedBlocksCommand.class,
            ValidateWrappedBlocksCommand.class,
            FetchBalanceCheckpointsCommand.class,
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
