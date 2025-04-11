// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.block.access;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running block access tests, including both positive and negative test
 * scenarios.
 *
 * <p>This suite aggregates the tests from {@link GetSingleBlockApiTests}. The {@code @Suite}
 * annotation allows running all selected classes in a single test run.
 */
@Suite
@SelectClasses({GetSingleBlockApiTests.class})
public class BlockAccessTestSuites {}
