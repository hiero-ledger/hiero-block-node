// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/** Unit tests for {@link BlocksCommand} via picocli. */
class BlocksCommandTest {

    // ===== Top-level command tests =====

    @Test
    void noSubcommand_printsUsageHelp() {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine cmd = new CommandLine(new BlocksCommand());
        cmd.setOut(new PrintWriter(out));
        cmd.setErr(new PrintWriter(err));

        int exitCode = cmd.execute();

        assertEquals(0, exitCode);
        String output = out.toString();
        assertTrue(output.contains("blocks"), "Should contain command name");
        assertTrue(output.contains("Works with block stream files"), "Should contain description");
    }

    @Test
    void help_printsUsageHelp() {
        StringWriter out = new StringWriter();
        CommandLine cmd = new CommandLine(new BlocksCommand());
        cmd.setOut(new PrintWriter(out));

        int exitCode = cmd.execute("--help");

        assertEquals(0, exitCode);
        String output = out.toString();
        assertTrue(output.contains("blocks"), "Should contain command name");
    }

    // ===== Subcommand registration tests =====

    @Test
    void allSubcommandsRegistered() {
        StringWriter out = new StringWriter();
        CommandLine cmd = new CommandLine(new BlocksCommand());
        cmd.setOut(new PrintWriter(out));

        cmd.execute("--help");

        String output = out.toString();
        assertTrue(output.contains("json"), "Should list 'json' subcommand");
        assertTrue(output.contains("ls"), "Should list 'ls' subcommand");
        assertTrue(output.contains("validate"), "Should list 'validate' subcommand");
        assertTrue(output.contains("wrap"), "Should list 'wrap' subcommand");
        assertTrue(output.contains("validate-wrapped"), "Should list 'validate-wrapped' subcommand");
    }

    // ===== Subcommand help tests =====

    @Test
    void validateHelp_printsUsage() {
        StringWriter out = new StringWriter();
        CommandLine cmd = new CommandLine(new BlocksCommand());
        cmd.setOut(new PrintWriter(out));

        int exitCode = cmd.execute("validate", "--help");

        assertEquals(0, exitCode);
        String output = out.toString();
        assertTrue(output.contains("validate"), "Should contain validate in output");
    }

    @Test
    void validateWrappedHelp_printsUsage() {
        StringWriter out = new StringWriter();
        CommandLine cmd = new CommandLine(new BlocksCommand());
        cmd.setOut(new PrintWriter(out));

        int exitCode = cmd.execute("validate-wrapped", "--help");

        assertEquals(0, exitCode);
        String output = out.toString();
        assertTrue(output.contains("validate-wrapped"), "Should contain validate-wrapped in output");
    }

    // ===== Subcommand no-args behavior =====

    @Test
    void json_noFiles_succeeds() {
        CommandLine cmd = new CommandLine(new BlocksCommand());
        // json with no files prints "No files to convert" to System.err but exits 0
        int exitCode = cmd.execute("json");
        assertEquals(0, exitCode);
    }

    @Test
    void ls_noFiles_succeeds() {
        CommandLine cmd = new CommandLine(new BlocksCommand());
        // ls with no files prints "No files to display info for" to System.err but exits 0
        int exitCode = cmd.execute("ls");
        assertEquals(0, exitCode);
    }
}
