// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber.positive;

import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.DisplayName;

/**
 * Test class for verifying correct behaviour of the application, when one subscriber requests data.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Single Subscriber Tests")
public class PositiveSingleSubscriberTests extends BaseSuite {
    /** Default constructor for the {@link PositiveSingleSubscriberTests} class. */
    public PositiveSingleSubscriberTests() {}

    public void shouldSubscribeForLiveBlocks() {}

    public void shouldSubscribeForHistoricalBlocks() {}

    public void shouldSubscribeForHistoricalAndLiveBlocks() {}
}
