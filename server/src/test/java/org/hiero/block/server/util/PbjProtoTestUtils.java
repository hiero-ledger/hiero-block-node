// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.util;

import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockAcknowledgement;
import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.PublishStreamRequestUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

public final class PbjProtoTestUtils {
    private PbjProtoTestUtils() {}

    public static Acknowledgement buildAck(@NonNull final Long blockNumber, Bytes blockHash) {

        BlockAcknowledgement blockAck = BlockAcknowledgement.newBuilder()
                .blockNumber(blockNumber)
                .blockRootHash(blockHash)
                .blockAlreadyExists(false)
                .build();

        return Acknowledgement.newBuilder().blockAck(blockAck).build();
    }

    public static Bytes buildEmptyPublishStreamRequest() {
        return PublishStreamRequestUnparsed.PROTOBUF.toBytes(PublishStreamRequestUnparsed.newBuilder()
                .blockItems(BlockItemSetUnparsed.newBuilder().build())
                .build());
    }

    public static Bytes buildEmptySubscribeStreamRequest() {
        return SubscribeStreamRequest.PROTOBUF.toBytes(
                SubscribeStreamRequest.newBuilder().build());
    }
}
