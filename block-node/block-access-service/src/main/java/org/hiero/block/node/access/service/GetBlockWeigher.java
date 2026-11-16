// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.function.LongSupplier;
import org.hiero.block.node.spi.throttle.ContentAwareWeigher;
import org.hiero.block.node.spi.throttle.WeightClass;

/// Classifies a `getBlock` request as [WeightClass#STANDARD] (live/recent) or [WeightClass#HEAVY]
/// (historical/archived), by how far the requested block sits behind the current maximum
/// available block.
///
/// A request for the latest block, or one that cannot be classified (malformed, or with no block
/// number), is always [WeightClass#STANDARD]: it is either inherently cheap or will be rejected as
/// invalid by [BlockAccessServicePlugin] itself.
///
/// Reads only the `block_number` field (field 1) directly off the wire, without a full parse of a
/// message the delegate parses again once the call is admitted.
final class GetBlockWeigher implements ContentAwareWeigher {
    private static final int BLOCK_NUMBER_FIELD = 1;
    /// Sentinel meaning the field was not on the wire (the oneof is `retrieve_latest` or unset).
    private static final long BLOCK_NUMBER_NOT_PRESENT = Long.MIN_VALUE;

    private final LongSupplier maxAvailableBlock;
    private final long historicalThresholdBlocks;

    /// @param maxAvailableBlock supplies the current maximum available block, read fresh per call
    /// @param historicalThresholdBlocks distance behind the tip beyond which a block is historical
    GetBlockWeigher(@NonNull final LongSupplier maxAvailableBlock, final long historicalThresholdBlocks) {
        this.maxAvailableBlock = maxAvailableBlock;
        this.historicalThresholdBlocks = historicalThresholdBlocks;
    }

    @NonNull
    @Override
    public WeightClass classify(@NonNull final Method method, @NonNull final Bytes requestBytes) {
        final long blockNumber;
        try {
            blockNumber = readBlockNumber(requestBytes);
        } catch (final IOException | RuntimeException e) {
            return WeightClass.STANDARD;
        }
        // A block number of -1 means "latest", the same as retrieve_latest.
        if (blockNumber == BLOCK_NUMBER_NOT_PRESENT || blockNumber < 0) {
            return WeightClass.STANDARD;
        }
        return maxAvailableBlock.getAsLong() - blockNumber > historicalThresholdBlocks
                ? WeightClass.HEAVY
                : WeightClass.STANDARD;
    }

    /// @return the `block_number` field's value, or [#BLOCK_NUMBER_NOT_PRESENT] if absent
    private static long readBlockNumber(@NonNull final Bytes requestBytes) throws IOException {
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
            if (field == BLOCK_NUMBER_FIELD) {
                return ProtoParserTools.readUint64(input);
            }
            ProtoParserTools.skipField(input, wireType);
        }
        return BLOCK_NUMBER_NOT_PRESENT;
    }
}
