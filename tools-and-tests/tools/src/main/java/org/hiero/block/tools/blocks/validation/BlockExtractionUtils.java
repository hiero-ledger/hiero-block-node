// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Instant;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;

/**
 * Shared utility methods for extracting data from unparsed blocks and transactions.
 */
public final class BlockExtractionUtils {

    private BlockExtractionUtils() {}

    /**
     * Extracts the block timestamp as an {@link Instant} from the block header.
     *
     * @param block the unparsed block
     * @return the block timestamp, or {@code null} if no block header is found
     * @throws ParseException if the block header cannot be parsed
     */
    public static Instant extractBlockInstant(final BlockUnparsed block) throws ParseException {
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasBlockHeader()) {
                final BlockHeader header = BlockHeader.PROTOBUF.parse(item.blockHeaderOrThrow());
                final Timestamp ts = header.blockTimestampOrThrow();
                return Instant.ofEpochSecond(ts.seconds(), ts.nanos());
            }
        }
        return null;
    }

    /**
     * Extracts the record file bytes from the block.
     *
     * @param block the unparsed block
     * @return the record file bytes, or {@code null} if no record file item is found
     */
    public static Bytes extractRecordFileBytes(final BlockUnparsed block) {
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasRecordFile()) {
                return item.recordFileOrThrow();
            }
        }
        return null;
    }

    /**
     * Extracts the {@link TransactionBody} from a transaction, handling all three encoding
     * formats: direct body, bodyBytes, and signedTransactionBytes.
     *
     * @param t the transaction
     * @return the parsed transaction body, or {@code null} if none could be extracted
     * @throws ParseException if parsing fails
     */
    public static TransactionBody extractTransactionBody(final Transaction t) throws ParseException {
        if (t.hasBody()) {
            return t.body();
        } else if (t.bodyBytes().length() > 0) {
            return TransactionBody.PROTOBUF.parse(t.bodyBytes());
        } else if (t.signedTransactionBytes().length() > 0) {
            final SignedTransaction st = SignedTransaction.PROTOBUF.parse(t.signedTransactionBytes());
            return TransactionBody.PROTOBUF.parse(st.bodyBytes());
        }
        return null;
    }
}
