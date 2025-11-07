// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class NetworkCapacityTest {

    @TempDir
    Path tempDir;

    private Path configFile;

    @BeforeEach
    void setUp() throws IOException {
        configFile = Files.createTempFile(tempDir, "network-capacity-config", ".json");
        Files.writeString(configFile, "{}");
    }

    @Test
    void serverModeWithoutPortPrintsHelpfulError() {
        ExecutionResult result = executeCommand("--mode", "server", "--config", configFile.toString());

        assertEquals(0, result.exitCode());
        assertTrue(
                result.stderr().contains("Error running NetworkCapacity command: Port is required for server mode"),
                () -> "stderr was:\n" + result.stderr());
    }

    @Test
    void clientModeWithoutRecordingFolderPrintsHelpfulError() {
        ExecutionResult result = executeCommand(
                "--mode",
                "client",
                "--config",
                configFile.toString(),
                "--port",
                "5041",
                "--serverAddress",
                "127.0.0.1");

        assertEquals(0, result.exitCode());
        assertTrue(
                result.stderr()
                        .contains(
                                "Error running NetworkCapacity command: Recording folder is required for client mode"),
                () -> "stderr was:\n" + result.stderr());
    }

    @Test
    void clientModeWithMissingFolderReportsError() {
        Path missingFolder = tempDir.resolve("missing-recordings");

        ExecutionResult result = executeCommand(
                "--mode",
                "client",
                "--config",
                configFile.toString(),
                "--port",
                "5041",
                "--serverAddress",
                "127.0.0.1",
                "--folder",
                missingFolder.toString());

        assertEquals(0, result.exitCode());
        assertTrue(
                result.stderr()
                        .contains(
                                "Error running NetworkCapacity command: Recording folder does not exist or is not a directory"),
                () -> "stderr was:\n" + result.stderr());
    }

    @Test
    void clientModeWithoutServerAddressPrintsHelpfulError() throws IOException {
        Path recordings = Files.createDirectory(tempDir.resolve("recordings"));

        ExecutionResult result = executeCommand(
                "--mode",
                "client",
                "--config",
                configFile.toString(),
                "--port",
                "5041",
                "--folder",
                recordings.toString());

        assertEquals(0, result.exitCode());
        assertTrue(
                result.stderr()
                        .contains("Error running NetworkCapacity command: Server address is required for client mode"),
                () -> "stderr was:\n" + result.stderr());
    }

    @Test
    void missingConfigFileIsReported() {
        Path missingConfig = tempDir.resolve("missing-config.json");

        ExecutionResult result =
                executeCommand("--mode", "server", "--config", missingConfig.toString(), "--port", "5041");

        assertEquals(0, result.exitCode());
        assertTrue(
                result.stderr().contains("Error running NetworkCapacity command: Configuration file does not exist"),
                () -> "stderr was:\n" + result.stderr());
    }

    @Test
    void invalidModeIsReported() {
        ExecutionResult result = executeCommand("--mode", "invalid", "--config", configFile.toString());

        assertEquals(0, result.exitCode());
        assertTrue(
                result.stderr().contains("Error running NetworkCapacity command: Invalid mode: invalid"),
                () -> "stderr was:\n" + result.stderr());
    }

    private ExecutionResult executeCommand(String... args) {
        CommandLine commandLine = new CommandLine(new NetworkCapacity());
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        PrintStream captureOut = new PrintStream(stdout);
        PrintStream captureErr = new PrintStream(stderr);
        int exitCode;
        try {
            System.setOut(captureOut);
            System.setErr(captureErr);
            exitCode = commandLine.execute(args);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            captureOut.close();
            captureErr.close();
        }

        return new ExecutionResult(
                exitCode, stdout.toString(StandardCharsets.UTF_8), stderr.toString(StandardCharsets.UTF_8));
    }

    private static final class ExecutionResult {
        private final int exitCode;
        private final String stdout;
        private final String stderr;

        private ExecutionResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        private int exitCode() {
            return exitCode;
        }

        private String stdout() {
            return stdout;
        }

        private String stderr() {
            return stderr;
        }
    }
}
