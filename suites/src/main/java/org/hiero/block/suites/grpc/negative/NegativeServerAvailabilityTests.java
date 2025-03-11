// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.grpc.negative;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

/**
 * Test class for verifying negative scenarios related to server availability for the Block Node
 * application. This class is part of the gRPC module and aims to test how the Block Node handles
 * incorrect configurations or failures during server startup.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Negative Server Availability Tests")
public class NegativeServerAvailabilityTests extends BaseSuite {

    /**
     * Default constructor for the {@link NegativeServerAvailabilityTests} class. This constructor
     * does not require any specific initialization.
     */
    public NegativeServerAvailabilityTests() {}

    /**
     * Clean up method executed after each test.
     *
     * <p>This method stops the running container, resets the container configuration by retrieving
     * a new one through {@link BaseSuite#createContainer()}, and then starts the Block Node
     * container again.
     */
    @AfterEach
    public void cleanUp() {
        blockNodeContainer.stop();
        blockNodeContainer = createContainer();
        blockNodeContainer.start();
    }

    /**
     * Test to verify that the Block Node server fails to start when provided with an invalid
     * configuration.
     *
     * <p>Specifically, this test modifies the environment variable "VERSION" with an invalid value,
     * which causes the server to fail during startup. The test expects a {@link
     * ContainerLaunchException} to be thrown.
     */
    @Test
    public void serverStartupThrowsForInvalidConfiguration() {
        blockNodeContainer.stop();
        blockNodeContainer.addEnv("VERSION", "Wrong!");
        assertThrows(
                ContainerLaunchException.class,
                () -> blockNodeContainer.start(),
                "Starting the Block Node container with invalid configuration should throw"
                        + " ContainerLaunchException.");
    }
}
