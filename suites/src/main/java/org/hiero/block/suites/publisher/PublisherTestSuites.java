// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.publisher;

import org.hiero.block.suites.publisher.positive.PositiveMultiplePublishersTests;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running multiple publishers tests, including both positive and negative test
 * scenarios.
 *
 * <p>This suite aggregates the tests from {@link PositiveMultiplePublishersTests}. The {@code @Suite}
 * annotation allows running all selected classes in a single test run.
 */
@Suite
@SelectClasses({PositiveMultiplePublishersTests.class})
public class PublisherTestSuites {
    /**
     * Default constructor for the {@link org.hiero.block.suites.publisher.PublisherTestSuites} class. This constructor is
     * empty as it does not need to perform any initialization.
     */
    public PublisherTestSuites() {}
}
