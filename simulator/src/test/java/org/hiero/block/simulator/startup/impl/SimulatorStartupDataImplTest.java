// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.from;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the {@link SimulatorStartupDataImpl} class.
 */
class SimulatorStartupDataImplTest {
    @TempDir
    private Path tempDir;

    private byte[] validSimulatedBlockHash;
    private Path latestAckBlockNumberPath;
    private Path latestAckBlockHashPath;
    private BlockGeneratorConfig blockGeneratorConfig;
    private SimulatorStartupDataConfig simulatorStartupDataConfig;
    private SimulatorStartupDataImpl toTest;

    @BeforeEach
    void setup() {
        validSimulatedBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        for (byte i = 0; i < StreamingTreeHasher.HASH_LENGTH; i++) {
            validSimulatedBlockHash[i] = i;
        }
        latestAckBlockNumberPath = tempDir.resolve("latestAckBlockNumber");
        latestAckBlockHashPath = tempDir.resolve("latestAckBlockHash");
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(BlockGeneratorConfig.class)
                .withConfigDataType(SimulatorStartupDataConfig.class)
                .withValue("simulator.startup.data.enabled", "true")
                .withValue("simulator.startup.data.latestAckBlockNumberPath", latestAckBlockNumberPath.toString())
                .withValue("simulator.startup.data.latestAckBlockHashPath", latestAckBlockHashPath.toString())
                .build();
        blockGeneratorConfig = configuration.getConfigData(BlockGeneratorConfig.class);
        simulatorStartupDataConfig = configuration.getConfigData(SimulatorStartupDataConfig.class);
        toTest = new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig);
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * return default values when the functionality is disabled.
     */
    @Test
    void testDefaultValues() {
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(BlockGeneratorConfig.class)
                .withConfigDataType(SimulatorStartupDataConfig.class)
                .build();
        final BlockGeneratorConfig generatorConfig = configuration.getConfigData(BlockGeneratorConfig.class);
        final SimulatorStartupDataConfig startupDataConfig =
                configuration.getConfigData(SimulatorStartupDataConfig.class);
        toTest = new SimulatorStartupDataImpl(startupDataConfig, generatorConfig);
        assertThat(toTest)
                .returns(-1L, from(SimulatorStartupDataImpl::getLatestAckBlockNumber))
                .returns(
                        new byte[StreamingTreeHasher.HASH_LENGTH],
                        from(SimulatorStartupDataImpl::getLatestAckBlockHash));
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * return default values when the functionality is enabled but no startup data
     * files exist (initial startup).
     */
    @Test
    void testDefaultValuesIfInitialStartup() {
        assertThat(latestAckBlockHashPath).isEmptyFile();
        assertThat(latestAckBlockNumberPath).isEmptyFile();
        assertThat(toTest)
                .returns(-1L, from(SimulatorStartupDataImpl::getLatestAckBlockNumber))
                .returns(
                        new byte[StreamingTreeHasher.HASH_LENGTH],
                        from(SimulatorStartupDataImpl::getLatestAckBlockHash));
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * return the correct values when the functionality is enabled and the startup
     * data files contain valid data.
     */
    @Test
    void testCorrectValuesStartup() throws IOException {
        Files.write(latestAckBlockNumberPath, "1".getBytes());
        Files.write(latestAckBlockHashPath, validSimulatedBlockHash);
        toTest = new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig);
        assertThat(toTest)
                .returns(1L, from(SimulatorStartupDataImpl::getLatestAckBlockNumber))
                .returns(validSimulatedBlockHash, from(SimulatorStartupDataImpl::getLatestAckBlockHash));
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * fail initialization if only the block number startup data file exists.
     */
    @Test
    void testFailedInitializationUnavailableHashFile() throws IOException {
        Files.write(latestAckBlockNumberPath, "1".getBytes());
        assertThat(latestAckBlockHashPath).isEmptyFile();
        assertThatIllegalStateException()
                .isThrownBy(() -> new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig));
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * fail initialization if only the block hash startup data file exists.
     */
    @Test
    void testFailedInitializationUnavailableBlockNumberFile() throws IOException {
        assertThat(latestAckBlockNumberPath).isEmptyFile();
        Files.write(latestAckBlockHashPath, validSimulatedBlockHash);
        assertThatIllegalStateException()
                .isThrownBy(() -> new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig));
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * fail initialization if the block number startup data file contains an
     * invalid number.
     */
    @Test
    void testFailedInitializationWrongNumberFormat() throws IOException {
        Files.write(latestAckBlockNumberPath, "wrongNumberFormat".getBytes());
        Files.write(latestAckBlockHashPath, validSimulatedBlockHash);
        assertThatExceptionOfType(NumberFormatException.class)
                .isThrownBy(() -> new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig));
    }

    /**
     * This test aims to verify that the {@link SimulatorStartupDataImpl} will
     * fail initialization if the block hash startup data file contains an invalid
     * hash length.
     */
    @Test
    void testFailedInitializationWrongHashLength() throws IOException {
        Files.write(latestAckBlockNumberPath, "1".getBytes());
        Files.write(latestAckBlockHashPath, new byte[StreamingTreeHasher.HASH_LENGTH - 1]);
        assertThatIllegalStateException()
                .isThrownBy(() -> new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig));
    }
}
