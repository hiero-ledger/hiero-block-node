// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Tests for {@link StatesCommand}. */
class StatesCommandTest {

    @Test
    void instantiation() {
        StatesCommand cmd = new StatesCommand();
        assertNotNull(cmd);
    }

    @Test
    void hasCommandAnnotation() {
        Command annotation = StatesCommand.class.getAnnotation(Command.class);
        assertNotNull(annotation);
        assertTrue(annotation.name().equals("states"));
    }

    @Test
    void runPrintsUsage() {
        // Running with no subcommand should print usage help without throwing
        assertDoesNotThrow(() -> new CommandLine(new StatesCommand()).execute());
    }
}
