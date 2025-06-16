// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.output.protoc.TransactionResult;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import com.hederahashgraph.api.proto.java.TokenTransferList;
import com.hederahashgraph.api.proto.java.TransferList;
import org.junit.jupiter.api.Test;

class TransactionResultHandlerTest {

    @Test
    void testGetItem() {
        TransactionResultHandler handler = new TransactionResultHandler();
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasTransactionResult());

        TransactionResult result = item.getTransactionResult();
        assertEquals(ResponseCodeEnum.SUCCESS, result.getStatus());
        assertNotNull(result.getConsensusTimestamp());
        assertNotNull(result.getTransferList());
        assertEquals(1, result.getTokenTransferListsCount());
    }

    @Test
    void testGetItemCaching() {
        TransactionResultHandler handler = new TransactionResultHandler();
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }

    @Test
    void testTransferList() {
        TransactionResultHandler handler = new TransactionResultHandler();
        TransferList transferList = handler.getItem().getTransactionResult().getTransferList();

        assertEquals(2, transferList.getAccountAmountsCount());

        AccountAmount debit = transferList.getAccountAmounts(0);
        AccountAmount credit = transferList.getAccountAmounts(1);

        assertTrue(debit.getAmount() < 0);
        assertTrue(credit.getAmount() > 0);
        assertEquals(-debit.getAmount(), credit.getAmount());

        assertTrue(debit.getAccountID().getAccountNum() >= 1);
        assertTrue(debit.getAccountID().getAccountNum() <= 100);
        assertTrue(credit.getAccountID().getAccountNum() >= 1);
        assertTrue(credit.getAccountID().getAccountNum() <= 100);
    }

    @Test
    void testTokenTransferList() {
        TransactionResultHandler handler = new TransactionResultHandler();
        TokenTransferList tokenTransfers =
                handler.getItem().getTransactionResult().getTokenTransferLists(0);

        assertNotNull(tokenTransfers.getToken());
        assertTrue(tokenTransfers.getToken().getTokenNum() >= 1);
        assertTrue(tokenTransfers.getToken().getTokenNum() <= 100);

        assertEquals(2, tokenTransfers.getTransfersCount());

        AccountAmount debit = tokenTransfers.getTransfers(0);
        AccountAmount credit = tokenTransfers.getTransfers(1);

        assertTrue(debit.getAmount() < 0);
        assertTrue(credit.getAmount() > 0);
        assertEquals(-debit.getAmount(), credit.getAmount());

        assertTrue(debit.getAccountID().getAccountNum() >= 1);
        assertTrue(debit.getAccountID().getAccountNum() <= 100);
        assertTrue(credit.getAccountID().getAccountNum() >= 1);
        assertTrue(credit.getAccountID().getAccountNum() <= 100);
    }
}
