// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.hedera.hapi.block.stream.output.protoc.TransactionResult;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import com.hederahashgraph.api.proto.java.TokenID;
import com.hederahashgraph.api.proto.java.TokenTransferList;
import com.hederahashgraph.api.proto.java.TransferList;
import org.hiero.block.common.utils.Preconditions;

/**
 * Handler for transaction results in the block stream.
 * Creates and manages transaction result items containing the outcome of transactions,
 * including transfer lists and token transfers.
 */
public class TransactionResultHandler extends AbstractBlockItemHandler {
    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem = BlockItem.newBuilder()
                    .setTransactionResult(createTransactionResult())
                    .build();
        }
        return blockItem;
    }

    private TransactionResult createTransactionResult() {
        return TransactionResult.newBuilder()
                .setStatus(ResponseCodeEnum.SUCCESS)
                .setTransferList(createTransferList())
                .addTokenTransferLists(createTokenTransferList())
                .setConsensusTimestamp(getTimestamp())
                .build();
    }

    private TransferList createTransferList() {
        long shardNum = generateRandomValue(1, 100);
        long realmNum = generateRandomValue(1, 100);
        long creditAccount = generateRandomValue(1, 100);
        long debitAccount = generateRandomValue(1, 100);
        long amount = generateRandomValue(100, 200);

        return TransferList.newBuilder()
                .addAccountAmounts(createAccountAmount(shardNum, realmNum, creditAccount, -amount))
                .addAccountAmounts(createAccountAmount(shardNum, realmNum, debitAccount, amount))
                .build();
    }

    private TokenTransferList createTokenTransferList() {
        long tokenId = generateRandomValue(1, 100);
        long shardNum = generateRandomValue(1, 100);
        long realmNum = generateRandomValue(1, 100);
        long creditAccount = generateRandomValue(1, 100);
        long debitAccount = generateRandomValue(1, 100);
        long amount = generateRandomValue(100, 200);

        return TokenTransferList.newBuilder()
                .setToken(TokenID.newBuilder()
                        .setShardNum(shardNum)
                        .setRealmNum(realmNum)
                        .setTokenNum(tokenId))
                .addTransfers(createAccountAmount(shardNum, realmNum, creditAccount, -amount))
                .addTransfers(createAccountAmount(shardNum, realmNum, debitAccount, amount))
                .build();
    }

    private AccountAmount createAccountAmount(
            final long shardNum, final long realmNum, final long accountNum, final long accountAmount) {
        Preconditions.requirePositive(shardNum);
        Preconditions.requirePositive(realmNum);
        Preconditions.requirePositive(accountNum);

        return AccountAmount.newBuilder()
                .setAccountID(AccountID.newBuilder()
                        .setShardNum(shardNum)
                        .setRealmNum(realmNum)
                        .setAccountNum(accountNum)
                        .build())
                .setAmount(accountAmount)
                .build();
    }
}
