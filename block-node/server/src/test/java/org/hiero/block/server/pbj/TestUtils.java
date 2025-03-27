// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import org.hiero.block.api.SubscribeStreamRequest;

public final class TestUtils {
    private TestUtils() {}

    static SubscribeStreamRequest buildSubscribeStreamRequest(long startBlockNumber, long endBlockNumber) {
        return SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .endBlockNumber(endBlockNumber)
                .build();
    }
}
