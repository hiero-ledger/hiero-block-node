// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.throttle.ContentAwareWeigher;
import org.hiero.block.node.spi.throttle.WeightClass;

/// Classifies a `subscribeBlockStream` request as [WeightClass#STANDARD] (live) or
/// [WeightClass#HEAVY] (historical), based on how far the requested start block sits behind the
/// current maximum available block. A pure live subscription (no fixed start block, encoded as
/// [org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER]) is always [WeightClass#STANDARD].
///
/// Reads only the `start_block_number` field (field 1) directly off the wire, without fully
/// deserializing the request — see `docs/design/apis/api-throttling.md` ("Performance") for why: a
/// weigher must not pay for a second full parse of a message the delegate will parse again in full
/// once admitted.
final class SubscribeStreamWeigher implements ContentAwareWeigher {
    private static final int START_BLOCK_NUMBER_FIELD = 1;

    private final HistoricalBlockFacility blockProvider;
    private final long historicalThresholdBlocks;

    SubscribeStreamWeigher(@NonNull final HistoricalBlockFacility blockProvider, final long historicalThresholdBlocks) {
        this.blockProvider = blockProvider;
        this.historicalThresholdBlocks = historicalThresholdBlocks;
    }

    @NonNull
    @Override
    public WeightClass classify(@NonNull final Method method, @NonNull final Bytes requestBytes) {
        final long startBlockNumber;
        try {
            startBlockNumber = readStartBlockNumber(requestBytes);
        } catch (final IOException e) {
            return WeightClass.STANDARD;
        }
        if (startBlockNumber == UNKNOWN_BLOCK_NUMBER) {
            // A pure live subscription (no fixed start block) — always live.
            return WeightClass.STANDARD;
        }
        final long maxAvailable = blockProvider.availableBlocks().max();
        final long distanceFromTip = maxAvailable - startBlockNumber;
        return distanceFromTip > historicalThresholdBlocks ? WeightClass.HEAVY : WeightClass.STANDARD;
    }

    /// Reads the `start_block_number` field directly from the wire, skipping every other field
    /// without decoding it. Returns {@code 0} (proto3's default) if the field is absent, matching
    /// what a full parse of the same bytes would return.
    private static long readStartBlockNumber(@NonNull final Bytes requestBytes) throws IOException {
        final ReadableSequentialData input = requestBytes.toReadableSequentialData();
        while (input.hasRemaining()) {
            final int tag;
            try {
                tag = input.readVarInt(false);
            } catch (final BufferUnderflowException e) {
                break;
            }
            final int field = tag >>> ProtoParserTools.TAG_FIELD_OFFSET;
            final ProtoConstants wireType = ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK);
            if (field == START_BLOCK_NUMBER_FIELD) {
                return ProtoParserTools.readUint64(input);
            }
            ProtoParserTools.skipField(input, wireType);
        }
        return 0L;
    }
}
