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

        assertTrue(isNumInRange(debit.getAccountID().getShardNum()));
        assertTrue(isNumInRange(debit.getAccountID().getRealmNum()));
        assertTrue(isNumInRange(debit.getAccountID().getAccountNum()));
        assertTrue(isNumInRange(credit.getAccountID().getShardNum()));
        assertTrue(isNumInRange(credit.getAccountID().getRealmNum()));
        assertTrue(isNumInRange(credit.getAccountID().getAccountNum()));
    }

    @Test
    void testTokenTransferList() {
        TransactionResultHandler handler = new TransactionResultHandler();
        TokenTransferList tokenTransfers =
                handler.getItem().getTransactionResult().getTokenTransferLists(0);

        assertNotNull(tokenTransfers.getToken());
        assertTrue(isNumInRange(tokenTransfers.getToken().getShardNum()));
        assertTrue(isNumInRange(tokenTransfers.getToken().getRealmNum()));
        assertTrue(isNumInRange(tokenTransfers.getToken().getTokenNum()));

        assertEquals(2, tokenTransfers.getTransfersCount());

        AccountAmount debit = tokenTransfers.getTransfers(0);
        AccountAmount credit = tokenTransfers.getTransfers(1);

        assertTrue(debit.getAmount() < 0);
        assertTrue(credit.getAmount() > 0);
        assertEquals(-debit.getAmount(), credit.getAmount());

        assertTrue(isNumInRange(debit.getAccountID().getShardNum()));
        assertTrue(isNumInRange(debit.getAccountID().getRealmNum()));
        assertTrue(isNumInRange(debit.getAccountID().getAccountNum()));
        assertTrue(isNumInRange(credit.getAccountID().getShardNum()));
        assertTrue(isNumInRange(credit.getAccountID().getRealmNum()));
        assertTrue(isNumInRange(credit.getAccountID().getAccountNum()));
    }

    private boolean isNumInRange(final long num) {
        return num >= 1 && num <= 100;
    }
}
