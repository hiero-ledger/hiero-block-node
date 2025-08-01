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
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;

/**
 * Handler for transaction results in the block stream.
 * Creates and manages transaction result items containing the outcome of transactions,
 * including transfer lists and token transfers.
 */
public class TransactionResultHandler extends AbstractBlockItemHandler {

    private final BlockGeneratorConfig blockGeneratorConfig;

    public TransactionResultHandler(@NonNull final BlockGeneratorConfig blockGeneratorConfig) {
        this.blockGeneratorConfig = blockGeneratorConfig;
    }

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
        long creditAccount = generateRandomValue(1, 100);
        long debitAccount = generateRandomValue(1, 100);
        long amount = generateRandomValue(100, 200);

        return TransferList.newBuilder()
                .addAccountAmounts(createAccountAmount(creditAccount, -amount))
                .addAccountAmounts(createAccountAmount(debitAccount, amount))
                .build();
    }

    private AccountAmount createAccountAmount(long accountNum, long accountAmount) {
        Preconditions.requirePositive(accountNum);
        // todo(700) Add support for non-zero shard/realm entity
        return AccountAmount.newBuilder()
                .setAccountID(AccountID.newBuilder()
                        .setRealmNum(blockGeneratorConfig.realmNum())
                        .setShardNum(blockGeneratorConfig.shardNum())
                        .setAccountNum(accountNum)
                        .build())
                .setAmount(accountAmount)
                .build();
    }

    private TokenTransferList createTokenTransferList() {
        long tokenId = generateRandomValue(1, 100);
        long creditAccount = generateRandomValue(1, 100);
        long debitAccount = generateRandomValue(1, 100);
        long amount = generateRandomValue(100, 200);

        return TokenTransferList.newBuilder()
                .setToken(TokenID.newBuilder()
                        .setRealmNum(blockGeneratorConfig.realmNum())
                        .setShardNum(blockGeneratorConfig.shardNum())
                        .setTokenNum(tokenId))
                .addTransfers(createAccountAmount(creditAccount, -amount))
                .addTransfers(createAccountAmount(debitAccount, amount))
                .build();
    }
}
