// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.grpc;

import org.hiero.block.suites.grpc.negative.NegativeServerAvailabilityTests;
import org.hiero.block.suites.grpc.positive.PositiveEndpointBehaviourTests;
import org.hiero.block.suites.grpc.positive.PositiveServerAvailabilityTests;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running gRPC server availability tests, including both positive and negative test
 * scenarios.
 *
 * <p>This suite aggregates the tests from {@link PositiveServerAvailabilityTests} and {@link
 * NegativeServerAvailabilityTests}. The {@code @Suite} annotation allows running all selected
 * classes in a single test run.
 */
@Suite
@SelectClasses({
    PositiveServerAvailabilityTests.class,
    PositiveEndpointBehaviourTests.class,
    NegativeServerAvailabilityTests.class
})
public class GrpcTestSuites {

    /**
     * Default constructor for the {@link GrpcTestSuites} class. This constructor is empty as it
     * does not need to perform any initialization.
     */
    public GrpcTestSuites() {}
}
