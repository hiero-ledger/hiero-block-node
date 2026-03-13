// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static com.hedera.pbj.runtime.ProtoConstants.TAG_WIRE_TYPE_MASK;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Lightweight protobuf extractor that navigates RecordFileItem bytes and extracts
 * only the transfer-related fields needed by {@link HbarSupplyValidation}.
 *
 * <p>This avoids fully parsing the expensive Transaction field (which contains the
 * full signed transaction body and signatures) and unused TransactionRecord fields
 * (receipt, hash, contract results, assessed fees, etc.), significantly reducing
 * object allocation per block.
 *
 * <p>The protobuf navigation follows the wire format directly, using field tags to
 * identify and skip unwanted fields while selectively parsing only:
 * <ul>
 *   <li>consensusTimestamp (for amendment merging)</li>
 *   <li>transferList (HBAR transfers)</li>
 *   <li>tokenTransferLists (fungible and NFT token transfers)</li>
 * </ul>
 */
final class TransferListExtractor {

    private TransferListExtractor() {} // utility class

    // ---- Visitor interface for zero-allocation extraction ----

    /** Callback interface for visiting transfer data without creating intermediate objects. */
    interface TransferVisitor {
        /** Called for each HBAR transfer in the transfer list. */
        void onHbarTransfer(long accountNum, long amount);

        /** Called for each fungible token transfer. */
        void onFungibleTokenTransfer(long accountNum, long tokenNum, long amount);

        /** Called for each NFT transfer. */
        void onNftTransfer(long senderNum, long receiverNum, long tokenNum, long serial);
    }

    // ---- Protobuf wire tags (field_number << 3 | wire_type) ----

    // RecordFileItem fields
    private static final int RFI_RECORD_FILE_CONTENTS = 18; // field 2, LEN
    private static final int RFI_AMENDMENTS = 34; // field 4, LEN

    // RecordStreamFile fields
    private static final int RSF_RECORD_STREAM_ITEMS = 26; // field 3, LEN

    // RecordStreamItem fields
    private static final int RSI_TRANSACTION = 10; // field 1, LEN (skip entirely)
    private static final int RSI_RECORD = 18; // field 2, LEN

    // TransactionRecord fields
    private static final int TR_CONSENSUS_TIMESTAMP = 26; // field 3, LEN
    private static final int TR_TRANSFER_LIST = 82; // field 10, LEN
    private static final int TR_TOKEN_TRANSFER_LISTS = 90; // field 11, LEN

    // Timestamp fields
    private static final int TS_SECONDS = 8; // field 1, VARINT
    private static final int TS_NANOS = 16; // field 2, VARINT

    // TransferList fields
    private static final int TL_ACCOUNT_AMOUNTS = 10; // field 1, LEN (repeated)

    // AccountAmount fields
    private static final int AA_ACCOUNT_ID = 10; // field 1, LEN
    private static final int AA_AMOUNT = 16; // field 2, SINT64

    // AccountID fields
    private static final int AID_ACCOUNT_NUM = 24; // field 3, VARINT

    // TokenTransferList fields
    private static final int TTL_TOKEN = 10; // field 1, LEN
    private static final int TTL_TRANSFERS = 18; // field 2, LEN (repeated)
    private static final int TTL_NFT_TRANSFERS = 26; // field 3, LEN (repeated)

    // TokenID fields
    private static final int TID_TOKEN_NUM = 24; // field 3, VARINT

    // NftTransfer fields
    private static final int NFT_SENDER = 10; // field 1, LEN
    private static final int NFT_RECEIVER = 18; // field 2, LEN
    private static final int NFT_SERIAL = 24; // field 3, VARINT

    // ---- Lightweight data records ----

    /** Lightweight HBAR or fungible token transfer (accountNum + amount). */
    record AccountTransfer(long accountNum, long amount) {}

    /** Lightweight NFT transfer. */
    record NftTransferInfo(long senderAccountNum, long receiverAccountNum, long serialNumber) {}

    /** Lightweight token transfer list for a single token. */
    record TokenTransferData(long tokenNum, List<AccountTransfer> transfers, List<NftTransferInfo> nftTransfers) {}

    /** Extracted transfer data from a single RecordStreamItem. */
    record TransferData(
            long consensusNanos, List<AccountTransfer> hbarTransfers, List<TokenTransferData> tokenTransfers) {}

    /** Extracted transfers from a RecordFileItem (original items + amendments). */
    record ExtractedTransfers(List<TransferData> items, List<TransferData> amendments) {}

    /**
     * Extracts transfer lists from raw RecordFileItem protobuf bytes, skipping
     * Transaction and unused TransactionRecord fields entirely.
     *
     * @param recordFileItemBytes the raw bytes of a RecordFileItem
     * @return the extracted transfer data
     * @throws ParseException if the protobuf structure is malformed
     */
    static ExtractedTransfers extract(final Bytes recordFileItemBytes) throws ParseException {
        final List<TransferData> items = new ArrayList<>();
        final List<TransferData> amendments = new ArrayList<>();
        final ReadableSequentialData input = recordFileItemBytes.toReadableSequentialData();
        try {
            while (input.hasRemaining()) {
                final int tag = readTag(input);
                if (tag == -1) break;
                switch (tag) {
                    case RFI_RECORD_FILE_CONTENTS -> {
                        final int len = input.readVarInt(false);
                        final ReadableSequentialData sub = input.view(len);
                        parseRecordStreamFile(sub, items);
                    }
                    case RFI_AMENDMENTS -> {
                        final int len = input.readVarInt(false);
                        final ReadableSequentialData sub = input.view(len);
                        amendments.add(parseRecordStreamItemTransfers(sub));
                    }
                    default -> skipTaggedField(input, tag);
                }
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e);
        }
        return new ExtractedTransfers(items, amendments);
    }

    /**
     * Extracts transfer lists from raw RecordFileItem protobuf bytes using a visitor
     * callback, avoiding all intermediate object allocation. Only usable when there
     * are no amendments (the common case). Returns {@code true} if extraction succeeded
     * via the visitor path, or {@code false} if amendments were present and the caller
     * should fall back to the list-based {@link #extract} method.
     *
     * @param recordFileItemBytes the raw bytes of a RecordFileItem
     * @param visitor the visitor to receive transfer callbacks
     * @return true if visitor path was used (no amendments), false if amendments present
     * @throws ParseException if the protobuf structure is malformed
     */
    static boolean extractInto(final Bytes recordFileItemBytes, final TransferVisitor visitor) throws ParseException {
        final ReadableSequentialData input = recordFileItemBytes.toReadableSequentialData();
        try {
            // First pass: capture the record contents view and check for amendments.
            // We must NOT call the visitor until we confirm no amendments, because field 2
            // (record contents) appears before field 4 (amendments) in wire order, and if
            // amendments exist the caller needs to fall back to the list-based merge path.
            ReadableSequentialData recordContents = null;
            while (input.hasRemaining()) {
                final int tag = readTag(input);
                if (tag == -1) break;
                switch (tag) {
                    case RFI_RECORD_FILE_CONTENTS -> {
                        final int len = input.readVarInt(false);
                        recordContents = input.view(len);
                    }
                    case RFI_AMENDMENTS -> {
                        // Amendments present — no visitor calls made, caller falls back
                        return false;
                    }
                    default -> skipTaggedField(input, tag);
                }
            }
            // No amendments — safe to visit the record contents
            if (recordContents != null) {
                visitRecordStreamFile(recordContents, visitor);
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e);
        }
        return true;
    }

    /**
     * Merges original items with amendments, sorted by consensus timestamp.
     * Amendments replace originals with the same timestamp.
     */
    static List<TransferData> mergeTransferData(
            final List<TransferData> original, final List<TransferData> amendments) {
        if (amendments.isEmpty()) return original;
        final Map<Long, TransferData> byTimestamp = new TreeMap<>();
        for (final TransferData item : original) {
            byTimestamp.put(item.consensusNanos(), item);
        }
        for (final TransferData amendment : amendments) {
            byTimestamp.put(amendment.consensusNanos(), amendment);
        }
        return new ArrayList<>(byTimestamp.values());
    }

    // ---- RecordStreamFile (field 2 of RecordFileItem) ----

    private static void parseRecordStreamFile(final ReadableSequentialData input, final List<TransferData> items)
            throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case RSF_RECORD_STREAM_ITEMS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    items.add(parseRecordStreamItemTransfers(sub));
                }
                default -> skipTaggedField(input, tag);
            }
        }
    }

    // ---- RecordStreamItem: skip Transaction, enter TransactionRecord ----

    private static TransferData parseRecordStreamItemTransfers(final ReadableSequentialData input) throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case RSI_TRANSACTION -> {
                    // Skip the entire Transaction (biggest allocation savings)
                    final int len = input.readVarInt(false);
                    input.skip(len);
                }
                case RSI_RECORD -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    return parseTransactionRecordTransfers(sub);
                }
                default -> skipTaggedField(input, tag);
            }
        }
        return new TransferData(0, List.of(), List.of());
    }

    // ---- TransactionRecord: extract only fields 3, 10, 11 ----

    private static TransferData parseTransactionRecordTransfers(final ReadableSequentialData input) throws Exception {
        long consensusNanos = 0;
        List<AccountTransfer> hbarTransfers = List.of();
        List<TokenTransferData> tokenTransfers = null;

        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TR_CONSENSUS_TIMESTAMP -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    consensusNanos = parseTimestampNanos(sub);
                }
                case TR_TRANSFER_LIST -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    hbarTransfers = parseTransferList(sub);
                }
                case TR_TOKEN_TRANSFER_LISTS -> {
                    if (tokenTransfers == null) tokenTransfers = new ArrayList<>();
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    tokenTransfers.add(parseTokenTransferList(sub));
                }
                default -> skipTaggedField(input, tag);
            }
        }
        return new TransferData(consensusNanos, hbarTransfers, tokenTransfers != null ? tokenTransfers : List.of());
    }

    // ---- Leaf-level parsers ----

    private static long parseTimestampNanos(final ReadableSequentialData input) throws Exception {
        long seconds = 0;
        int nanos = 0;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TS_SECONDS -> seconds = input.readVarLong(false);
                case TS_NANOS -> nanos = input.readVarInt(false);
                default -> skipTaggedField(input, tag);
            }
        }
        return seconds * 1_000_000_000L + nanos;
    }

    private static List<AccountTransfer> parseTransferList(final ReadableSequentialData input) throws Exception {
        final List<AccountTransfer> amounts = new ArrayList<>();
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TL_ACCOUNT_AMOUNTS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    amounts.add(parseAccountAmount(sub));
                }
                default -> skipTaggedField(input, tag);
            }
        }
        return amounts;
    }

    private static AccountTransfer parseAccountAmount(final ReadableSequentialData input) throws Exception {
        long accountNum = 0;
        long amount = 0;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case AA_ACCOUNT_ID -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    accountNum = parseAccountIdNum(sub);
                }
                case AA_AMOUNT -> amount = input.readVarLong(true); // SINT64, zigzag
                default -> skipTaggedField(input, tag);
            }
        }
        return new AccountTransfer(accountNum, amount);
    }

    private static long parseAccountIdNum(final ReadableSequentialData input) throws Exception {
        long accountNum = 0;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case AID_ACCOUNT_NUM -> accountNum = input.readVarLong(false);
                default -> skipTaggedField(input, tag);
            }
        }
        return accountNum;
    }

    private static TokenTransferData parseTokenTransferList(final ReadableSequentialData input) throws Exception {
        long tokenNum = 0;
        List<AccountTransfer> transfers = null;
        List<NftTransferInfo> nftTransfers = null;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TTL_TOKEN -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    tokenNum = parseTokenIdNum(sub);
                }
                case TTL_TRANSFERS -> {
                    if (transfers == null) transfers = new ArrayList<>();
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    transfers.add(parseAccountAmount(sub));
                }
                case TTL_NFT_TRANSFERS -> {
                    if (nftTransfers == null) nftTransfers = new ArrayList<>();
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    nftTransfers.add(parseNftTransfer(sub));
                }
                default -> skipTaggedField(input, tag);
            }
        }
        return new TokenTransferData(
                tokenNum, transfers != null ? transfers : List.of(), nftTransfers != null ? nftTransfers : List.of());
    }

    private static long parseTokenIdNum(final ReadableSequentialData input) throws Exception {
        long tokenNum = 0;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TID_TOKEN_NUM -> tokenNum = input.readVarLong(false);
                default -> skipTaggedField(input, tag);
            }
        }
        return tokenNum;
    }

    private static NftTransferInfo parseNftTransfer(final ReadableSequentialData input) throws Exception {
        long senderAccountNum = 0;
        long receiverAccountNum = 0;
        long serialNumber = 0;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case NFT_SENDER -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    senderAccountNum = parseAccountIdNum(sub);
                }
                case NFT_RECEIVER -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    receiverAccountNum = parseAccountIdNum(sub);
                }
                case NFT_SERIAL -> serialNumber = input.readVarLong(false);
                default -> skipTaggedField(input, tag);
            }
        }
        return new NftTransferInfo(senderAccountNum, receiverAccountNum, serialNumber);
    }

    // ---- Visitor-based parsers (zero allocation) ----

    private static void visitRecordStreamFile(final ReadableSequentialData input, final TransferVisitor visitor)
            throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case RSF_RECORD_STREAM_ITEMS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    visitRecordStreamItemTransfers(sub, visitor);
                }
                default -> skipTaggedField(input, tag);
            }
        }
    }

    private static void visitRecordStreamItemTransfers(
            final ReadableSequentialData input, final TransferVisitor visitor) throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case RSI_TRANSACTION -> {
                    final int len = input.readVarInt(false);
                    input.skip(len);
                }
                case RSI_RECORD -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    visitTransactionRecordTransfers(sub, visitor);
                }
                default -> skipTaggedField(input, tag);
            }
        }
    }

    private static void visitTransactionRecordTransfers(
            final ReadableSequentialData input, final TransferVisitor visitor) throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TR_TRANSFER_LIST -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    visitTransferList(sub, visitor);
                }
                case TR_TOKEN_TRANSFER_LISTS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    visitTokenTransferList(sub, visitor);
                }
                default -> skipTaggedField(input, tag);
            }
        }
    }

    private static void visitTransferList(final ReadableSequentialData input, final TransferVisitor visitor)
            throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TL_ACCOUNT_AMOUNTS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    long accountNum = 0;
                    long amount = 0;
                    while (sub.hasRemaining()) {
                        final int aaTag = readTag(sub);
                        if (aaTag == -1) break;
                        switch (aaTag) {
                            case AA_ACCOUNT_ID -> {
                                final int idLen = sub.readVarInt(false);
                                final ReadableSequentialData idSub = sub.view(idLen);
                                accountNum = parseAccountIdNum(idSub);
                            }
                            case AA_AMOUNT -> amount = sub.readVarLong(true);
                            default -> skipTaggedField(sub, aaTag);
                        }
                    }
                    visitor.onHbarTransfer(accountNum, amount);
                }
                default -> skipTaggedField(input, tag);
            }
        }
    }

    private static void visitTokenTransferList(final ReadableSequentialData input, final TransferVisitor visitor)
            throws Exception {
        long tokenNum = 0;
        // First pass: find the token ID (may appear after transfers in the wire format,
        // but in practice token_id is field 1 and comes first)
        // We need tokenNum before visiting transfers, so we do a two-pass approach:
        // read token ID first, then re-read for transfers. However, since ReadableSequentialData
        // is forward-only, we collect data in a single pass using deferred visitor calls.
        // Actually, protobuf field ordering is typically ascending, so tokenId (field 1)
        // comes before transfers (field 2) and nftTransfers (field 3). We rely on this.
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case TTL_TOKEN -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    tokenNum = parseTokenIdNum(sub);
                }
                case TTL_TRANSFERS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    long accountNum = 0;
                    long amount = 0;
                    while (sub.hasRemaining()) {
                        final int aaTag = readTag(sub);
                        if (aaTag == -1) break;
                        switch (aaTag) {
                            case AA_ACCOUNT_ID -> {
                                final int idLen = sub.readVarInt(false);
                                final ReadableSequentialData idSub = sub.view(idLen);
                                accountNum = parseAccountIdNum(idSub);
                            }
                            case AA_AMOUNT -> amount = sub.readVarLong(true);
                            default -> skipTaggedField(sub, aaTag);
                        }
                    }
                    visitor.onFungibleTokenTransfer(accountNum, tokenNum, amount);
                }
                case TTL_NFT_TRANSFERS -> {
                    final int len = input.readVarInt(false);
                    final ReadableSequentialData sub = input.view(len);
                    long senderAccountNum = 0;
                    long receiverAccountNum = 0;
                    long serialNumber = 0;
                    while (sub.hasRemaining()) {
                        final int nftTag = readTag(sub);
                        if (nftTag == -1) break;
                        switch (nftTag) {
                            case NFT_SENDER -> {
                                final int sLen = sub.readVarInt(false);
                                final ReadableSequentialData sSub = sub.view(sLen);
                                senderAccountNum = parseAccountIdNum(sSub);
                            }
                            case NFT_RECEIVER -> {
                                final int rLen = sub.readVarInt(false);
                                final ReadableSequentialData rSub = sub.view(rLen);
                                receiverAccountNum = parseAccountIdNum(rSub);
                            }
                            case NFT_SERIAL -> serialNumber = sub.readVarLong(false);
                            default -> skipTaggedField(sub, nftTag);
                        }
                    }
                    visitor.onNftTransfer(senderAccountNum, receiverAccountNum, tokenNum, serialNumber);
                }
                default -> skipTaggedField(input, tag);
            }
        }
    }

    // ---- Protobuf wire format utilities ----

    /** Reads a protobuf tag, returning -1 on EOF. */
    private static int readTag(final ReadableSequentialData input) {
        if (!input.hasRemaining()) return -1;
        try {
            return input.readVarInt(false);
        } catch (EOFException e) {
            return -1;
        }
    }

    /** Skips a field value based on the wire type encoded in the tag. */
    private static void skipTaggedField(final ReadableSequentialData input, final int tag) throws Exception {
        final int wireType = tag & TAG_WIRE_TYPE_MASK;
        ProtoParserTools.skipField(input, ProtoConstants.get(wireType));
    }
}
