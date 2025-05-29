// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber;

import org.hiero.block.suites.subscriber.negative.NegativeSingleSubscriberTests;
import org.hiero.block.suites.subscriber.positive.PositiveSingleSubscriberTests;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite for running subscriber tests, including both positive and negative test scenarios.
 *
 * <p> This suite aggregates the tests related to the subscriber functionality. The {@code @Suite} annotation allows running all selected classes in a single test run.</p>
 */
@Suite
@SelectClasses({PositiveSingleSubscriberTests.class, NegativeSingleSubscriberTests.class})
public class SubscriberTestSuites {
    /**
     * Default constructor for the {@link PositiveSingleSubscriberTests} class.This constructor is
     * empty as it does not need to perform any initialization.
     */
    public SubscriberTestSuites() {}
}
