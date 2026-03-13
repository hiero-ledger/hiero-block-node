// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.node.base.AccountAmount;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NftTransfer;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.TokenID;
import com.hedera.hapi.node.base.TokenTransferList;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.base.TransferList;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.AccountTransfer;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.ExtractedTransfers;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.NftTransferInfo;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TokenTransferData;
import org.hiero.block.tools.blocks.validation.TransferListExtractor.TransferData;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TransferListExtractor}, verifying that the lightweight custom
 * protobuf extraction produces the same results as full PBJ parsing.
 */
class TransferListExtractorTest {

    // ── Helper builders ──

    private static AccountID acctId(long num) {
        return AccountID.newBuilder().accountNum(num).build();
    }

    private static TokenID tokenId(long num) {
        return TokenID.newBuilder().tokenNum(num).build();
    }

    private static AccountAmount acctAmt(long accountNum, long amount) {
        return AccountAmount.newBuilder()
                .accountID(acctId(accountNum))
                .amount(amount)
                .build();
    }

    private static RecordStreamItem buildRecordStreamItem(
            long seconds, int nanos, List<AccountAmount> hbarTransfers, List<TokenTransferList> tokenTransfers) {
        TransactionRecord.Builder recordBuilder = TransactionRecord.newBuilder()
                .consensusTimestamp(
                        Timestamp.newBuilder().seconds(seconds).nanos(nanos).build())
                .transferList(
                        TransferList.newBuilder().accountAmounts(hbarTransfers).build());
        if (tokenTransfers != null && !tokenTransfers.isEmpty()) {
            recordBuilder.tokenTransferLists(tokenTransfers);
        }
        return RecordStreamItem.newBuilder()
                .transaction(Transaction.DEFAULT)
                .record(recordBuilder.build())
                .build();
    }

    private static Bytes toRecordFileItemBytes(List<RecordStreamItem> items, List<RecordStreamItem> amendments) {
        RecordFileItem.Builder builder = RecordFileItem.newBuilder()
                .creationTime(Timestamp.newBuilder().seconds(1L).build())
                .recordFileContents(
                        RecordStreamFile.newBuilder().recordStreamItems(items).build());
        if (amendments != null && !amendments.isEmpty()) {
            builder.amendments(amendments);
        }
        return RecordFileItem.PROTOBUF.toBytes(builder.build());
    }

    // ── Tests ──

    @Test
    void emptyRecordFile_extractsEmptyLists() throws ParseException {
        Bytes bytes = toRecordFileItemBytes(List.of(), List.of());
        ExtractedTransfers result = TransferListExtractor.extract(bytes);
        assertTrue(result.items().isEmpty());
        assertTrue(result.amendments().isEmpty());
    }

    @Test
    void singleHbarTransfer_matchesFullParse() throws ParseException {
        RecordStreamItem rsi = buildRecordStreamItem(100, 500, List.of(acctAmt(2, 1000), acctAmt(3, -1000)), List.of());
        Bytes bytes = toRecordFileItemBytes(List.of(rsi), List.of());

        // Full parse for reference
        RecordFileItem fullParsed = RecordFileItem.PROTOBUF.parse(bytes);
        List<AccountAmount> fullAmounts = fullParsed
                .recordFileContentsOrThrow()
                .recordStreamItems()
                .getFirst()
                .recordOrThrow()
                .transferListOrThrow()
                .accountAmounts();

        // Lightweight extraction
        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        assertEquals(1, extracted.items().size());
        assertTrue(extracted.amendments().isEmpty());

        TransferData td = extracted.items().getFirst();
        assertEquals(100_000_000_500L, td.consensusNanos());
        assertEquals(fullAmounts.size(), td.hbarTransfers().size());

        for (int i = 0; i < fullAmounts.size(); i++) {
            AccountAmount expected = fullAmounts.get(i);
            AccountTransfer actual = td.hbarTransfers().get(i);
            assertEquals(expected.accountIDOrThrow().accountNumOrThrow(), actual.accountNum());
            assertEquals(expected.amount(), actual.amount());
        }
    }

    @Test
    void negativeAmounts_zigzagDecodedCorrectly() throws ParseException {
        // SINT64 uses zigzag encoding; verify negative amounts round-trip correctly
        RecordStreamItem rsi = buildRecordStreamItem(
                1,
                0,
                List.of(
                        acctAmt(2, -5_000_000_000L),
                        acctAmt(3, 5_000_000_000L),
                        acctAmt(4, -1),
                        acctAmt(5, Long.MIN_VALUE + 1),
                        acctAmt(6, Long.MAX_VALUE)),
                List.of());
        Bytes bytes = toRecordFileItemBytes(List.of(rsi), List.of());

        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        List<AccountTransfer> transfers = extracted.items().getFirst().hbarTransfers();
        assertEquals(5, transfers.size());
        assertEquals(-5_000_000_000L, transfers.get(0).amount());
        assertEquals(5_000_000_000L, transfers.get(1).amount());
        assertEquals(-1, transfers.get(2).amount());
        assertEquals(Long.MIN_VALUE + 1, transfers.get(3).amount());
        assertEquals(Long.MAX_VALUE, transfers.get(4).amount());
    }

    @Test
    void fungibleTokenTransfers_matchFullParse() throws ParseException {
        TokenTransferList ttl = TokenTransferList.newBuilder()
                .token(tokenId(100))
                .transfers(acctAmt(2, 500), acctAmt(3, -500))
                .build();
        RecordStreamItem rsi = buildRecordStreamItem(10, 0, List.of(acctAmt(98, 1)), List.of(ttl));
        Bytes bytes = toRecordFileItemBytes(List.of(rsi), List.of());

        // Full parse
        RecordFileItem fullParsed = RecordFileItem.PROTOBUF.parse(bytes);
        TokenTransferList fullTtl = fullParsed
                .recordFileContentsOrThrow()
                .recordStreamItems()
                .getFirst()
                .recordOrThrow()
                .tokenTransferLists()
                .getFirst();

        // Lightweight
        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        TransferData td = extracted.items().getFirst();
        assertEquals(1, td.tokenTransfers().size());

        TokenTransferData ttd = td.tokenTransfers().getFirst();
        assertEquals(fullTtl.tokenOrThrow().tokenNum(), ttd.tokenNum());
        assertEquals(fullTtl.transfers().size(), ttd.transfers().size());
        for (int i = 0; i < fullTtl.transfers().size(); i++) {
            assertEquals(
                    fullTtl.transfers().get(i).accountIDOrThrow().accountNumOrThrow(),
                    ttd.transfers().get(i).accountNum());
            assertEquals(
                    fullTtl.transfers().get(i).amount(), ttd.transfers().get(i).amount());
        }
        assertTrue(ttd.nftTransfers().isEmpty());
    }

    @Test
    void nftTransfers_matchFullParse() throws ParseException {
        NftTransfer nft = NftTransfer.newBuilder()
                .senderAccountID(acctId(10))
                .receiverAccountID(acctId(20))
                .serialNumber(42)
                .build();
        TokenTransferList ttl = TokenTransferList.newBuilder()
                .token(tokenId(200))
                .nftTransfers(nft)
                .build();
        RecordStreamItem rsi = buildRecordStreamItem(5, 999, List.of(acctAmt(98, 0)), List.of(ttl));
        Bytes bytes = toRecordFileItemBytes(List.of(rsi), List.of());

        // Full parse
        RecordFileItem fullParsed = RecordFileItem.PROTOBUF.parse(bytes);
        NftTransfer fullNft = fullParsed
                .recordFileContentsOrThrow()
                .recordStreamItems()
                .getFirst()
                .recordOrThrow()
                .tokenTransferLists()
                .getFirst()
                .nftTransfers()
                .getFirst();

        // Lightweight
        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        TokenTransferData ttd = extracted.items().getFirst().tokenTransfers().getFirst();
        assertEquals(1, ttd.nftTransfers().size());
        NftTransferInfo nftInfo = ttd.nftTransfers().getFirst();
        assertEquals(fullNft.senderAccountIDOrThrow().accountNumOrThrow(), nftInfo.senderAccountNum());
        assertEquals(fullNft.receiverAccountIDOrThrow().accountNumOrThrow(), nftInfo.receiverAccountNum());
        assertEquals(fullNft.serialNumber(), nftInfo.serialNumber());
    }

    @Test
    void multipleRecordStreamItems_allExtracted() throws ParseException {
        RecordStreamItem rsi1 = buildRecordStreamItem(1, 0, List.of(acctAmt(2, 100)), List.of());
        RecordStreamItem rsi2 = buildRecordStreamItem(2, 0, List.of(acctAmt(3, 200)), List.of());
        RecordStreamItem rsi3 = buildRecordStreamItem(3, 0, List.of(acctAmt(4, 300)), List.of());
        Bytes bytes = toRecordFileItemBytes(List.of(rsi1, rsi2, rsi3), List.of());

        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        assertEquals(3, extracted.items().size());
        assertEquals(1_000_000_000L, extracted.items().get(0).consensusNanos());
        assertEquals(2_000_000_000L, extracted.items().get(1).consensusNanos());
        assertEquals(3_000_000_000L, extracted.items().get(2).consensusNanos());
        assertEquals(100, extracted.items().get(0).hbarTransfers().getFirst().amount());
        assertEquals(200, extracted.items().get(1).hbarTransfers().getFirst().amount());
        assertEquals(300, extracted.items().get(2).hbarTransfers().getFirst().amount());
    }

    @Test
    void amendments_extractedSeparately() throws ParseException {
        RecordStreamItem original = buildRecordStreamItem(1, 0, List.of(acctAmt(2, 100)), List.of());
        RecordStreamItem amendment = buildRecordStreamItem(1, 0, List.of(acctAmt(2, 999)), List.of());
        Bytes bytes = toRecordFileItemBytes(List.of(original), List.of(amendment));

        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        assertEquals(1, extracted.items().size());
        assertEquals(1, extracted.amendments().size());
        assertEquals(
                100, extracted.items().getFirst().hbarTransfers().getFirst().amount());
        assertEquals(
                999,
                extracted.amendments().getFirst().hbarTransfers().getFirst().amount());
    }

    @Test
    void mergeTransferData_amendmentReplacesOriginal() {
        TransferData original = new TransferData(1_000_000_000L, List.of(new AccountTransfer(2, 100)), List.of());
        TransferData amendment = new TransferData(1_000_000_000L, List.of(new AccountTransfer(2, 999)), List.of());

        List<TransferData> merged = TransferListExtractor.mergeTransferData(List.of(original), List.of(amendment));
        assertEquals(1, merged.size());
        assertEquals(999, merged.getFirst().hbarTransfers().getFirst().amount());
    }

    @Test
    void mergeTransferData_emptyAmendments_returnsOriginal() {
        TransferData td1 = new TransferData(1_000_000_000L, List.of(new AccountTransfer(2, 100)), List.of());
        TransferData td2 = new TransferData(2_000_000_000L, List.of(new AccountTransfer(3, 200)), List.of());

        List<TransferData> merged = TransferListExtractor.mergeTransferData(List.of(td1, td2), List.of());
        // Should return the exact same list instance
        assertEquals(2, merged.size());
    }

    @Test
    void mergeTransferData_sortsByTimestamp() {
        TransferData td1 = new TransferData(3_000_000_000L, List.of(new AccountTransfer(2, 300)), List.of());
        TransferData td2 = new TransferData(1_000_000_000L, List.of(new AccountTransfer(3, 100)), List.of());
        TransferData amendment = new TransferData(2_000_000_000L, List.of(new AccountTransfer(4, 200)), List.of());

        List<TransferData> merged = TransferListExtractor.mergeTransferData(List.of(td1, td2), List.of(amendment));
        assertEquals(3, merged.size());
        assertEquals(1_000_000_000L, merged.get(0).consensusNanos());
        assertEquals(2_000_000_000L, merged.get(1).consensusNanos());
        assertEquals(3_000_000_000L, merged.get(2).consensusNanos());
    }

    @Test
    void mixedFungibleAndNft_allExtractedCorrectly() throws ParseException {
        // Build a complex RecordStreamItem with HBAR + fungible + NFT transfers
        TokenTransferList fungible = TokenTransferList.newBuilder()
                .token(tokenId(50))
                .transfers(acctAmt(10, 1000), acctAmt(11, -1000))
                .build();
        NftTransfer nft1 = NftTransfer.newBuilder()
                .senderAccountID(acctId(20))
                .receiverAccountID(acctId(21))
                .serialNumber(1)
                .build();
        NftTransfer nft2 = NftTransfer.newBuilder()
                .senderAccountID(acctId(20))
                .receiverAccountID(acctId(22))
                .serialNumber(2)
                .build();
        TokenTransferList nftList = TokenTransferList.newBuilder()
                .token(tokenId(60))
                .nftTransfers(nft1, nft2)
                .build();
        RecordStreamItem rsi = buildRecordStreamItem(
                42, 123, List.of(acctAmt(2, 5000), acctAmt(3, -5000)), List.of(fungible, nftList));
        Bytes bytes = toRecordFileItemBytes(List.of(rsi), List.of());

        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        TransferData td = extracted.items().getFirst();

        // Verify consensus timestamp
        assertEquals(42_000_000_123L, td.consensusNanos());

        // Verify HBAR transfers
        assertEquals(2, td.hbarTransfers().size());
        assertEquals(2, td.hbarTransfers().get(0).accountNum());
        assertEquals(5000, td.hbarTransfers().get(0).amount());
        assertEquals(3, td.hbarTransfers().get(1).accountNum());
        assertEquals(-5000, td.hbarTransfers().get(1).amount());

        // Verify fungible token transfers
        assertEquals(2, td.tokenTransfers().size());
        TokenTransferData fungibleData = td.tokenTransfers().get(0);
        assertEquals(50, fungibleData.tokenNum());
        assertEquals(2, fungibleData.transfers().size());
        assertEquals(10, fungibleData.transfers().get(0).accountNum());
        assertEquals(1000, fungibleData.transfers().get(0).amount());
        assertTrue(fungibleData.nftTransfers().isEmpty());

        // Verify NFT transfers
        TokenTransferData nftData = td.tokenTransfers().get(1);
        assertEquals(60, nftData.tokenNum());
        assertTrue(nftData.transfers().isEmpty());
        assertEquals(2, nftData.nftTransfers().size());
        assertEquals(20, nftData.nftTransfers().get(0).senderAccountNum());
        assertEquals(21, nftData.nftTransfers().get(0).receiverAccountNum());
        assertEquals(1, nftData.nftTransfers().get(0).serialNumber());
        assertEquals(20, nftData.nftTransfers().get(1).senderAccountNum());
        assertEquals(22, nftData.nftTransfers().get(1).receiverAccountNum());
        assertEquals(2, nftData.nftTransfers().get(1).serialNumber());
    }

    @Test
    void hbarDeltaSum_matchesFullParse() throws ParseException {
        // Build several items with various transfers and verify total HBAR delta matches
        RecordStreamItem rsi1 = buildRecordStreamItem(
                1, 0, List.of(acctAmt(2, 1_000_000L), acctAmt(3, -500_000L), acctAmt(98, -500_000L)), List.of());
        RecordStreamItem rsi2 =
                buildRecordStreamItem(2, 0, List.of(acctAmt(4, 250_000L), acctAmt(5, -250_000L)), List.of());
        Bytes bytes = toRecordFileItemBytes(List.of(rsi1, rsi2), List.of());

        // Full parse delta
        RecordFileItem full = RecordFileItem.PROTOBUF.parse(bytes);
        long fullDelta = 0;
        for (RecordStreamItem rsi : full.recordFileContentsOrThrow().recordStreamItems()) {
            for (AccountAmount aa : rsi.recordOrThrow().transferListOrThrow().accountAmounts()) {
                fullDelta += aa.amount();
            }
        }

        // Lightweight delta
        ExtractedTransfers extracted = TransferListExtractor.extract(bytes);
        long lightDelta = 0;
        for (TransferData td : extracted.items()) {
            for (AccountTransfer at : td.hbarTransfers()) {
                lightDelta += at.amount();
            }
        }

        assertEquals(fullDelta, lightDelta);
    }
}
