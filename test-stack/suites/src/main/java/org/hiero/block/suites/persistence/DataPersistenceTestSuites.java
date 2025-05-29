// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.persistence;

import org.hiero.block.suites.persistence.positive.PositiveDataPersistenceTests;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running data persistence tests, including both positive and negative test
 * scenarios.
 *
 * <p>This suite aggregates the tests from {@link PositiveDataPersistenceTests}. The {@code @Suite}
 * annotation allows running all selected classes in a single test run.
 */
@Suite
@SelectClasses({PositiveDataPersistenceTests.class})
public class DataPersistenceTestSuites {

    /**
     * Default constructor for the {@link DataPersistenceTestSuites} class. This constructor is
     * empty as it does not need to perform any initialization.
     */
    public DataPersistenceTestSuites() {}
}
