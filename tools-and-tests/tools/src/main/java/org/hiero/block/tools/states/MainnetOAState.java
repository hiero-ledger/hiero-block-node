package org.hiero.block.tools.states;

import static org.hiero.block.tools.states.SavedStateConverter.convertStateToStateChanges;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import picocli.CommandLine.Command;

/**
 * Subcommand to read a saved state at the start of Hedera Mainnet Open Access (round 33485415)
 */
@Command(
    name = "oa-state",
    description = "Load and print the state at the start of Hedera Mainnet Open Access (round 33485415)")
public class MainnetOAState implements Runnable {
    public static String STATE_33485415_DIR = "saved-state-33485415";

    public static List<BlockItem> loadOaState() throws Exception {
        return convertStateToStateChanges(STATE_33485415_DIR);
    }

    @Override
    public void run() {
        try {
            System.out.println(Block.JSON.toJSON(new Block(loadOaState())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
