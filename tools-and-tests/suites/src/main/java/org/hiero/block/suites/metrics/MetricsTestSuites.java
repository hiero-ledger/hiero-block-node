// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.metrics;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running metrics-related tests.
 *
 * <p>This suite aggregates the tests from {@link MetricsCommonTests}. The {@code @Suite}
 * annotation allows running all selected classes in a single test run.
 */
@Suite
@SelectClasses({
    MetricsCommonTests.class,
    // Add other metrics test classes here as needed
})
public class MetricsTestSuites {}
