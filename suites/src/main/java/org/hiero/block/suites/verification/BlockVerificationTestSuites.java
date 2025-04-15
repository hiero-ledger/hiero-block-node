// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.verification;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running block verification tests, including both positive and negative test
 * scenarios.
 *
 * <p>This suite aggregates the tests from {@link VerificationCommonTests}. The {@code @Suite}
 * annotation allows running all selected classes in a single test run.
 */
@Suite
@SelectClasses({VerificationCommonTests.class})
public class BlockVerificationTestSuites {}
