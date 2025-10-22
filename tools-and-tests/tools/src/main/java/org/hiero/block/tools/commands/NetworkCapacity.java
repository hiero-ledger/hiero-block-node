// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.tools.capacity.NetworkCapacityClient;
import org.hiero.block.tools.capacity.NetworkCapacityServer;
import org.hiero.block.tools.config.HelidonWebClientConfig;
import org.hiero.block.tools.config.HelidonWebServerConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "networkCapacity", description = "Calculate network capacity based on block stream data")
public class NetworkCapacity implements Runnable {
    @Option(
            names = {"-m", "--mode"},
            description = "mode, either server or client",
            required = true)
    private String mode;

    @Option(
            names = {"-c", "--config"},
            description = "JSON configuration file path",
            required = true)
    private Path configFile;

    @Option(
            names = {"-p", "--port"},
            description = "If in server mode, the port to listen on, if in client mode the port to connect to",
            required = false)
    private Integer port;

    @Option(
            names = {"-f", "--folder"},
            description = "Recording folder (required for client mode)",
            required = false)
    private Path recordingFolder;

    @Option(
            names = {"-s", "--serverAddress"},
            description = "Server address (required for client mode)",
            required = false)
    private String serverAddress;

    @Override
    public void run() {
        try {
            validateParameters();
            // Print the mode
            System.out.println("Running in " + mode + " mode");
            // print the config file path
            System.out.println("Using configuration file: " + configFile);
            // print the config details
            System.out.println("Configuration file:" + Files.readString(configFile));

            if ("server".equalsIgnoreCase(mode)) {
                runServer();
            } else if ("client".equalsIgnoreCase(mode)) {
                runClient();
            } else {
                throw new IllegalArgumentException("Invalid mode: " + mode + ". Must be 'server' or 'client'");
            }
        } catch (Exception e) {
            String errorMsg = "Error running NetworkCapacity command: " + e.getMessage();
            System.err.println(errorMsg);
            e.printStackTrace();
        }
    }

    private void validateParameters() {
        if (!Files.exists(configFile)) {
            throw new IllegalArgumentException("Configuration file does not exist: " + configFile);
        }

        if ("server".equalsIgnoreCase(mode) && port == null) {
            throw new IllegalArgumentException("Port is required for server mode");
        }

        if ("client".equalsIgnoreCase(mode) && recordingFolder == null) {
            throw new IllegalArgumentException("Recording folder is required for client mode");
        }

        if ("client".equalsIgnoreCase(mode)
                && (!Files.exists(recordingFolder) || !Files.isDirectory(recordingFolder))) {
            throw new IllegalArgumentException(
                    "Recording folder does not exist or is not a directory: " + recordingFolder);
        }

        if ("client".equalsIgnoreCase(mode) && serverAddress == null) {
            throw new IllegalArgumentException("Server address is required for client mode");
        }
    }

    private void runServer() throws IOException, ParseException {
        System.out.println("Starting in Server Mode");

        HelidonWebServerConfig webServerConfig =
                HelidonWebServerConfig.JSON.parse(Bytes.wrap(Files.readAllBytes(configFile)));

        // Log the config loaded
        System.out.println("Client configuration loaded: " + webServerConfig);

        NetworkCapacityServer server = new NetworkCapacityServer(port, webServerConfig);
        server.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        // Keep the main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Network Capacity Server interrupted, shutting down.");
        }
    }

    private void runClient() throws IOException, ParseException {
        System.out.println("Starting in Client Mode");

        HelidonWebClientConfig clientConfig =
                HelidonWebClientConfig.JSON.parse(Bytes.wrap(Files.readAllBytes(configFile)));

        // Log the config loaded
        System.out.println("Client configuration loaded: " + clientConfig);

        NetworkCapacityClient client = new NetworkCapacityClient(clientConfig, recordingFolder, serverAddress, port);
        client.run();
    }
}
