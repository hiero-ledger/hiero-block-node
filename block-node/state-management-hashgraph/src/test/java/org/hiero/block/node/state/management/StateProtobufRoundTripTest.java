// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.StateMetadata;
import org.junit.jupiter.api.Test;

/**
 * Round-trips the newly-introduced protobuf messages to guard against an
 * accidental schema break in shared_message_types.proto or state_service.proto.
 * Grouped into a single test per message family — these are mechanical
 * structural checks, not behaviour.
 */
class StateProtobufRoundTripTest {

    @Test
    void stateMetadataAndQueryMessagesRoundTrip() throws Exception {
        final StateMetadata md = StateMetadata.newBuilder()
                .blockNumber(42L)
                .roundNumber(7L)
                .stateRootHash(Bytes.fromHex("deadbeef"))
                .stateSize(1024L)
                .build();
        final StateMetadata parsedMd = StateMetadata.PROTOBUF.parse(StateMetadata.PROTOBUF.toBytes(md));
        assertThat(parsedMd).isEqualTo(md);

        final BinaryStateQuery query = BinaryStateQuery.newBuilder()
                .blockNumber(42L)
                .stateId(9L)
                .keyBytes(Bytes.fromHex("01020304"))
                .queueIndex(0L)
                .build();
        final BinaryStateQuery parsedQuery = BinaryStateQuery.PROTOBUF.parse(BinaryStateQuery.PROTOBUF.toBytes(query));
        assertThat(parsedQuery).isEqualTo(query);

        final BinaryStateQueryResponse response = BinaryStateQueryResponse.newBuilder()
                .status(BinaryStateQueryResponse.Code.SUCCESS)
                .stateMetadata(md)
                .kvBytes(Bytes.fromHex("cafebabe"))
                .build();
        final BinaryStateQueryResponse parsedResponse =
                BinaryStateQueryResponse.PROTOBUF.parse(BinaryStateQueryResponse.PROTOBUF.toBytes(response));
        assertThat(parsedResponse).isEqualTo(response);
        assertThat(parsedResponse.stateMetadataOrThrow().blockNumber()).isEqualTo(42L);
    }
}
