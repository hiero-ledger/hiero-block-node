// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import org.hiero.block.simulator.config.SimulatorMappedConfigSourceInitializer;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.hiero.block.simulator.metrics.MetricsService;

/** The BlockStreamSimulator class defines the simulator for the block stream. */
public class BlockStreamSimulator {
    private static final Logger LOGGER = System.getLogger(BlockStreamSimulator.class.getName());

    /** This constructor should not be instantiated. */
    private BlockStreamSimulator() {}

    /**
     * The main entry point for the block stream simulator.
     *
     * @param args the arguments to be passed to the block stream simulator
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted
     * @throws BlockSimulatorParsingException if a parse error occurs
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the hash algorithm
     */
    public static void main(final String[] args)
            throws IOException, InterruptedException, BlockSimulatorParsingException, NoSuchAlgorithmException {

        LOGGER.log(INFO, "Starting Block Stream Simulator!");

        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withSource(SimulatorMappedConfigSourceInitializer.getMappedConfigSource())
                .withSource(SystemEnvironmentConfigSource.getInstance())
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSource(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
                .autoDiscoverExtensions();

        final Configuration configuration = configurationBuilder.build();

        final BlockStreamSimulatorInjectionComponent DIComponent =
                DaggerBlockStreamSimulatorInjectionComponent.factory().create(configuration);

        // Close the metrics service on JVM shutdown so the metrics exporter (e.g. Prometheus HTTP
        // server) is stopped. Only metrics are closed here: the mode handlers already shut down their
        // gRPC clients when streaming ends, and closing them twice would fail.
        final MetricsService metricsService = DIComponent.getMetricsService();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            try {
                                metricsService.close();
                            } catch (final IOException e) {
                                LOGGER.log(ERROR, "Failed to close metrics service", e);
                            }
                        },
                        "simulator-metrics-shutdown"));

        final BlockStreamSimulatorApp blockStreamSimulatorApp = DIComponent.getBlockStreamSimulatorApp();
        blockStreamSimulatorApp.start();
    }
}
