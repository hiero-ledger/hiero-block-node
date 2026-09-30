// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.hedera.pbj.runtime.grpc.GrpcCall;
import com.hedera.pbj.runtime.grpc.GrpcClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BlockStreamSubscribeUnparsedClientTest {

    @Test
    @DisplayName("getBatchOfBlocks times out and fails when no response is ever received")
    void awaitTimesOutWhenNoResponseIsReceived() {
        final GrpcClient grpcClient = mock(GrpcClient.class);
        final GrpcCall<?, ?> grpcCall = mock(GrpcCall.class);
        // Never invoke the pipeline callbacks, so RequestContext.await() has nothing to wake it up
        // other than the timeout itself.
        doReturn(grpcCall).when(grpcClient).createCall(any(), any(), any(), any(), anyMap());

        final BlockStreamSubscribeUnparsedClient client = new BlockStreamSubscribeUnparsedClient(grpcClient, 50L);

        assertThatThrownBy(() -> client.getBatchOfBlocks(1, 2))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Error fetching blocks")
                .cause()
                .hasMessage("Timed out waiting for block stream response");
    }
}
