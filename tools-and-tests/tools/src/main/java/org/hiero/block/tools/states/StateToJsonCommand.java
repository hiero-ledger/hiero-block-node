// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.io.File;
import java.util.List;
import org.hiero.block.tools.states.model.CompleteSavedState;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Subcommand to read a saved state and print its contents as a block stream block of state changes
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
@Command(name = "state-to-json", description = "Convert a saved state directory to JSON representation")
public class StateToJsonCommand implements Runnable {
    @Parameters(index = "0..*", description = "Saves state directories to process")
    private final File[] savedStateDirectories = new File[0];

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (savedStateDirectories.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }
        for (File savedStateDir : savedStateDirectories) {
            final CompleteSavedState completeSavedState = SavedStateConverter.loadState(savedStateDir.toPath());
            final List<BlockItem> blockItems = SavedStateConverter.signedStateToStateChanges(completeSavedState);
            System.out.println(Block.JSON.toJSON(new Block(blockItems)));
        }
    }
}
