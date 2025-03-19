// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import com.hedera.hapi.block.SubscribeStreamRequest;

public final class TestUtils {
    private TestUtils() {}

    static SubscribeStreamRequest buildSubscribeStreamRequest(long startBlockNumber, long endBlockNumber) {
        return SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .endBlockNumber(endBlockNumber)
                .build();
    }
}
