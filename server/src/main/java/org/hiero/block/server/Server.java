// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import java.io.IOException;
import java.nio.file.Path;
import org.hiero.block.server.config.ServerMappedConfigSourceInitializer;

/** Main class for the block node server */
public class Server {

    private static final Logger LOGGER = System.getLogger(Server.class.getName());

    private Server() {}

    /**
     * Main entrypoint for the block node server
     *
     * @param args Command line arguments. Not used at present.
     * @throws IOException if there is an error starting the server
     */
    public static void main(final String[] args) throws IOException {
        LOGGER.log(INFO, "Starting BlockNode Server");

        // Init BlockNode Configuration
        final Configuration configuration = ConfigurationBuilder.create()
                .withSource(ServerMappedConfigSourceInitializer.getMappedConfigSource())
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSources(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
                .autoDiscoverExtensions()
                .build();

        // Init Dagger DI Component, passing in the configuration.
        // this is where all the dependencies are wired up (magic happens)
        final BlockNodeAppInjectionComponent daggerComponent =
                DaggerBlockNodeAppInjectionComponent.factory().create(configuration);

        // Use Dagger DI Component to start the BlockNodeApp with all wired dependencies
        final BlockNodeApp blockNodeApp = daggerComponent.getBlockNodeApp();
        blockNodeApp.start();
    }
}
