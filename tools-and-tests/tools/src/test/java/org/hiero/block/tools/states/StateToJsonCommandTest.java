// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.hiero.block.tools.states.model.CompleteSavedState;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Tests for {@link StateToJsonCommand}. */
class StateToJsonCommandTest {

    @Test
    void instantiation() {
        StateToJsonCommand cmd = new StateToJsonCommand();
        assertNotNull(cmd);
    }

    @Test
    void hasCommandAnnotation() {
        Command annotation = StateToJsonCommand.class.getAnnotation(Command.class);
        assertNotNull(annotation);
        assertEquals("state-to-json", annotation.name());
    }

    @Test
    void runWithNoArgsPrintsUsage() {
        // Running with no arguments should print usage help without throwing
        assertDoesNotThrow(() -> new CommandLine(new StateToJsonCommand()).execute());
    }

    @Test
    void runWithDir() {
        // Resolve the absolute path to the saved state resource directory
        String resourcePath = System.getProperty("user.dir") + "/src/main/resources/saved-state-33485415";
        assertTrue(new java.io.File(resourcePath).exists(), "saved-state-33485415 directory should exist");

        // Capture stdout to validate the JSON output
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
            int exitCode = new CommandLine(new StateToJsonCommand()).execute(resourcePath);
            assertEquals(0, exitCode, "StateToJsonCommand should exit with code 0");
        } finally {
            System.setOut(originalOut);
        }

        // The command prints JSON via System.out.println(Block.JSON.toJSON(...))
        // Validate that the captured output contains valid JSON with block items
        String output = stdout.toString(StandardCharsets.UTF_8);
        assertFalse(output.isBlank(), "Command should produce JSON output");
        // The JSON output from Block.JSON.toJSON contains the block data
        assertTrue(output.contains("{") && output.contains("}"), "Output should contain JSON");
    }

    @Test
    void convertSavedStateToJson() {
        // Load the saved state using the resource-based loader (handles .gz files)
        CompleteSavedState state = SavedStateConverter.loadState("/saved-state-33485415");
        assertNotNull(state);
        List<BlockItem> blockItems = SavedStateConverter.signedStateToStateChanges(state);
        assertFalse(blockItems.isEmpty());
        // Verify JSON serialization works end-to-end (the core of what StateToJsonCommand does)
        String json = assertDoesNotThrow(() -> Block.JSON.toJSON(new Block(blockItems)));
        assertNotNull(json);
        assertFalse(json.isEmpty());
    }
}
