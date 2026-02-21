// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import com.hedera.hapi.streams.AllAccountBalances;
import com.hedera.hapi.streams.SingleAccountBalances;
import com.hedera.hapi.streams.TokenUnitBalance;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Parser for AllAccountBalances protobuf using PBJ with increased size limits.
 * Extracts both HBAR balances and token balances from the protobuf data.
 *
 * <p>Uses PBJ's 5-argument parse method to specify a larger maxSize (256MB) to handle
 * mainnet balance files which can contain millions of accounts.
 */
public class BalanceProtobufParser {

    /** Maximum parse depth for nested protobuf messages. */
    private static final int MAX_DEPTH = 512;

    /** Maximum size for parsing balance files (256MB to handle mainnet's ~8M accounts). */
    private static final int MAX_SIZE = 256 * 1024 * 1024;

    /**
     * Parse AllAccountBalances protobuf and return HBAR balances as a Map.
     *
     * @param pbBytes the protobuf bytes
     * @return map of account number to tinybar balance
     * @throws IOException if parsing fails
     */
    public static Map<Long, Long> parseToMap(byte[] pbBytes) throws IOException {
        final AllAccountBalances balances = parse(pbBytes);
        final Map<Long, Long> result = new HashMap<>();
        for (final SingleAccountBalances account : balances.allAccounts()) {
            final long accountNum = account.accountIDOrThrow().accountNumOrThrow();
            result.put(accountNum, account.hbarBalance());
        }
        return result;
    }

    /**
     * Parse AllAccountBalances protobuf and return the full parsed object.
     * This includes both HBAR balances and token balances.
     *
     * @param pbBytes the protobuf bytes
     * @return the parsed AllAccountBalances
     * @throws IOException if parsing fails
     */
    public static AllAccountBalances parse(byte[] pbBytes) throws IOException {
        try {
            return AllAccountBalances.PROTOBUF.parse(
                    Bytes.wrap(pbBytes).toReadableSequentialData(),
                    false, // strictMode
                    false, // parseUnknownFields
                    MAX_DEPTH,
                    MAX_SIZE);
        } catch (com.hedera.pbj.runtime.ParseException e) {
            throw new IOException("Failed to parse AllAccountBalances protobuf", e);
        }
    }

    /**
     * Parse AllAccountBalances and populate both HBAR and token balance maps.
     *
     * @param pbBytes the protobuf bytes
     * @param hbarBalances map to populate with account number to tinybar balance
     * @param tokenBalances map to populate with (accountNum, tokenNum) to balance
     * @throws IOException if parsing fails
     */
    public static void parseWithTokens(
            byte[] pbBytes, Map<Long, Long> hbarBalances, Map<Long, Map<Long, Long>> tokenBalances) throws IOException {
        final AllAccountBalances balances = parse(pbBytes);

        for (final SingleAccountBalances account : balances.allAccounts()) {
            final long accountNum = account.accountIDOrThrow().accountNumOrThrow();

            // HBAR balance
            hbarBalances.put(accountNum, account.hbarBalance());

            // Token balances
            if (!account.tokenUnitBalances().isEmpty()) {
                final Map<Long, Long> accountTokens = tokenBalances.computeIfAbsent(accountNum, k -> new HashMap<>());
                for (final TokenUnitBalance tokenBalance : account.tokenUnitBalances()) {
                    accountTokens.put(tokenBalance.tokenIdOrThrow().tokenNum(), tokenBalance.balance());
                }
            }
        }
    }
}
