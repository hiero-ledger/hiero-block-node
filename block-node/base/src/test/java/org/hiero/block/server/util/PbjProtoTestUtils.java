// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.util;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.Acknowledgement;
import org.hiero.block.api.BlockAcknowledgement;
import org.hiero.block.api.BlockItemSetUnparsed;
import org.hiero.block.api.PublishStreamRequestUnparsed;
import org.hiero.block.api.SubscribeStreamRequest;

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

    public static Bytes buildLiveStreamSubscribeStreamRequest() {
        return SubscribeStreamRequest.PROTOBUF.toBytes(SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0L)
                .endBlockNumber(0L)
                .build());
    }
}
