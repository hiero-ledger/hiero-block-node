// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Top level command for working with saved state directories containing SignedState.swh etc.
 */
@Command(
        name = "records",
        description = "Tools for working with saved states directories",
        subcommands = {
            StateToJsonCommand.class,
            MainnetOAState.class,
        },
        mixinStandardHelpOptions = true)
public class StatesCommand implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // Use picocli to print the usage help (which includes subcommands) when no subcommand is specified
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
