// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.server.service.Constants.PBJ_PROTOCOL_PROVIDER_CONFIG_NAME;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import com.swirlds.metrics.api.Metrics;
import io.helidon.common.Builder;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.server.config.ServerMappedConfigSourceInitializer;
import org.hiero.block.server.config.logging.ConfigurationLoggingImpl;
import org.hiero.block.server.plugins.BlockNodeContext;
import org.hiero.block.server.plugins.BlockNodePlugin;
import org.hiero.block.server.plugins.blockmessaging.BlockMessagingFacility;
import org.hiero.block.server.plugins.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.server.plugins.health.HealthFacility;

/** Main class for the block node server */
public class Server implements HealthFacility {

    private static final Logger LOGGER = System.getLogger(Server.class.getName());

    private final BlockNodeContext blockNodeContext;
    private final List<BlockNodePlugin> loadedPlugins;
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    private final WebServer webServer;

    private Server() throws IOException {
        // Init BlockNode Configuration
        final Configuration configuration = ConfigurationBuilder.create()
                .withSource(ServerMappedConfigSourceInitializer.getMappedConfigSource())
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSources(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
                .autoDiscoverExtensions()
                .build();

        // Init Metrics
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();

        // Create HistoricalBlockFacilityImpl
        final HistoricalBlockFacility blockProvider = new HistoricalBlockFacilityImpl(configuration);

        // Load Block Messaging Service plugin - for now allow nulls
        final BlockMessagingFacility blockMessagingService = ServiceLoader
                .load(BlockMessagingFacility.class, Server.class.getClassLoader())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No BlockMessagingFacility provided"));

        // Build block node context
        blockNodeContext = new BlockNodeContext() {
                    @Override
                    public Configuration configuration() {
                        return configuration;
                    }

                    @Override
                    public Metrics metrics() {
                        return metrics;
                    }

                    @Override
                    public HealthFacility serverHealth() {
                        return Server.this;
                    }

                    @Override
                    public BlockMessagingFacility blockMessaging() {
                        return blockMessagingService;
                    }

                    @Override
                    public HistoricalBlockFacility historicalBlockProvider() {
                        return blockProvider;
                    }

                    @Override
                    public void shutdown() {
                        Server.this.shutdown();
                    }
                };
        // Load all the plugins
        loadedPlugins = ServiceLoader.load(BlockNodePlugin.class, Server.class.getClassLoader())
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        // load and Log the configuration
        var configurationLogging = new ConfigurationLoggingImpl(configuration);
        configurationLogging.log();


        final HttpRouting.Builder httpRouting =
                HttpRouting.builder().register(healthService.getHealthRootPath(), healthService);

        final PbjRouting.Builder pbjRouting =
                PbjRouting.builder().service(pbjBlockStreamService).service(pbjBlockAccessService);

        // Override the default message size
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name(PBJ_PROTOCOL_PROVIDER_CONFIG_NAME)
                .maxMessageSizeBytes(serverConfig.maxMessageSizeBytes())
                .build();

        final ConnectionConfig connectionConfig = ConnectionConfig.builder()
                .sendBufferSize(serverConfig.socketSendBufferSizeBytes())
                .receiveBufferSize(serverConfig.socketSendBufferSizeBytes())
                .build();

        // Build the web server
        final var webServerBuilder = WebServerConfig.builder();
        final WebServer webServer = webServerBuilder
                .port(serverConfig.port())
                .addProtocol(pbjConfig)
                .addRouting(pbjRouting)
                .addRouting(httpRouting)
                .connectionConfig(connectionConfig)
                .build();


    }

    private void start() {
        // Start all the plugins
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "Starting plugin: {0}", plugin.name());
            final List<Builder<?, ? extends Routing>> routingBuilders = plugin.start(blockNodeContext);
            for (Builder<?, ? extends Routing> routingBuilder : routingBuilders) {
                HttpRouting.builder().register(routingBuilder);
            }
        }

        // Start the web server
        webServer.start();
        // Start metrics
        blockNodeContext.metrics().start();
        // mark the server as started
        state.set(State.RUNNING);
    }


    private void shutdown() {
        state.set(State.SHUTTING_DOWN);
        LOGGER.log(INFO, "Shutting down");
        // Stop server
        // Stop all the plugins
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "Stopping plugin: {0}", plugin.name());
            plugin.stop();
        }
        LOGGER.log(INFO, "Bye bye");
    }

    /**
     * Main entrypoint for the block node server
     *
     * @param args Command line arguments. Not used at present.
     * @throws IOException if there is an error starting the server
     */
    public static void main(final String[] args) throws IOException {
        LOGGER.log(INFO, "Starting BlockNode Server");
//
//        // Init BlockNode Configuration
//        final Configuration configuration = ConfigurationBuilder.create()
//                .withSource(ServerMappedConfigSourceInitializer.getMappedConfigSource())
//                .withSource(SystemPropertiesConfigSource.getInstance())
//                .withSources(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
//                .autoDiscoverExtensions()
//                .build();
//
//        // Init Dagger DI Component, passing in the configuration.
//        // this is where all the dependencies are wired up (magic happens)
//        final BlockNodeAppInjectionComponent daggerComponent =
//                DaggerBlockNodeAppInjectionComponent.factory().create(configuration);
//
//        // Use Dagger DI Component to start the BlockNodeApp with all wired dependencies
//        final BlockNodeApp blockNodeApp = daggerComponent.getBlockNodeApp();
//        blockNodeApp.start();

        Server server = new Server();

    }

    @Override
    public State status() {
        return null;
    }
}
