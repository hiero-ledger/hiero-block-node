// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import com.hedera.hapi.streams.AllAccountBalances;
import com.hedera.hapi.streams.SingleAccountBalances;
import com.hedera.hapi.streams.TokenUnitBalance;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Tracks running account balances (HBAR, fungible tokens, and NFTs) across a sequence of wrapped
 * blocks.
 *
 * <p>This class maintains an in-memory map from account number to {@link Account}, which records:
 * <ul>
 *   <li>The tinybar HBAR balance.
 *   <li>Fungible-token balances: {@code tokenNum → units}.
 *   <li>NFT ownership: {@code tokenNum → Set<serialNumber>} — each NFT is uniquely identified by
 *       its token type ({@link com.hedera.hapi.node.base.TokenID}) and serial number, so ownership
 *       is tracked as the set of serial numbers held per token type.
 * </ul>
 *
 * <p>Balances are updated from two sources during block processing:
 * <ol>
 *   <li>{@code StateChanges} items – {@link #setHbarBalance} sets an absolute HBAR balance;
 *       {@link #deleteAccount} removes the account entirely.
 *   <li>{@code RecordFile} items – {@link #applyHbarChange} applies a relative HBAR delta;
 *       {@link #applyFungibleTokenChange} applies a relative fungible-token delta;
 *       {@link #applyNftTransfer} moves a specific NFT serial number from sender to receiver.
 * </ol>
 *
 * <p>Use {@link #totalHbarBalance()} to compute the sum of all HBAR balances (for the
 * 50-billion-HBAR supply check), and {@link #compare(AllAccountBalances)} to validate the
 * running state against a balance snapshot read from disk.
 */
public class RunningAccountsState {

    private static final Logger LOGGER = Logger.getLogger(RunningAccountsState.class.getName());

    /** Per-account state: HBAR balance, fungible token units, and owned NFT serial numbers. */
    private static class Account {
        long tinyBarBalance = 0;
        /** tokenNum → fungible units held. */
        final Map<Long, Long> fungibleBalances = new HashMap<>();
        /** tokenNum → set of NFT serial numbers owned. */
        final Map<Long, Set<Long>> nftSerials = new HashMap<>();
    }

    private final Map<Long, Account> accounts = new HashMap<>();

    /**
     * Sets the HBAR balance for an account to an absolute value (from StateChanges map update).
     * Creates the account entry if it does not yet exist.
     *
     * @param accountNum     the account number
     * @param tinyBarBalance the new absolute tinybar balance
     */
    public void setHbarBalance(final long accountNum, final long tinyBarBalance) {
        accounts.computeIfAbsent(accountNum, k -> new Account()).tinyBarBalance = tinyBarBalance;
    }

    /**
     * Applies a relative HBAR balance change to an account (from RecordFile transfer list).
     * Creates the account entry if it does not yet exist.
     *
     * @param accountNum    the account number
     * @param tinyBarChange the signed delta in tinybar (positive = credit, negative = debit)
     */
    public void applyHbarChange(final long accountNum, final long tinyBarChange) {
        accounts.computeIfAbsent(accountNum, k -> new Account()).tinyBarBalance += tinyBarChange;
    }

    /**
     * Creates an account entry with zero balances if one does not already exist.
     *
     * @param accountIdNum the account number
     */
    public void createAccount(final long accountIdNum) {
        accounts.putIfAbsent(accountIdNum, new Account());
    }

    /**
     * Removes an account entry (from StateChanges map delete).
     *
     * @param accountIdNum the account number
     */
    public void deleteAccount(final long accountIdNum) {
        accounts.remove(accountIdNum);
    }

    /**
     * Applies a relative fungible-token balance change to an account.
     * Creates the account entry if it does not yet exist.
     *
     * @param accountNum the account number
     * @param tokenNum   the token number
     * @param amount     the signed delta (positive = receive, negative = send)
     */
    public void applyFungibleTokenChange(final long accountNum, final long tokenNum, final long amount) {
        final Account account = accounts.computeIfAbsent(accountNum, k -> new Account());
        account.fungibleBalances.merge(tokenNum, amount, Long::sum);
    }

    /**
     * Records one NFT transfer by moving the specific serial number from the sender's ownership
     * set to the receiver's ownership set. Creates account entries if they do not yet exist.
     *
     * <p>Each NFT is a unique asset identified by the combination of {@code tokenNum} and
     * {@code serialNumber}. Multiple NFTs of the same token type are tracked as a set of serial
     * numbers, not as a count.
     *
     * @param senderAccountNum   the account number of the current NFT owner
     * @param receiverAccountNum the account number of the new NFT owner
     * @param tokenNum           the token type number
     * @param serialNumber       the unique serial number of the NFT being transferred
     */
    public void applyNftTransfer(
            final long senderAccountNum, final long receiverAccountNum, final long tokenNum, final long serialNumber) {
        final Account sender = accounts.computeIfAbsent(senderAccountNum, k -> new Account());
        sender.nftSerials.computeIfAbsent(tokenNum, k -> new HashSet<>()).remove(serialNumber);

        final Account receiver = accounts.computeIfAbsent(receiverAccountNum, k -> new Account());
        receiver.nftSerials.computeIfAbsent(tokenNum, k -> new HashSet<>()).add(serialNumber);
    }

    /**
     * Returns the sum of all HBAR balances across all tracked accounts.
     *
     * <p>Used by the 50-billion-HBAR supply check in
     * {@link WrappedBlockValidator#validate50Billion}.
     *
     * @return total tinybar balance across all accounts
     */
    public long totalHbarBalance() {
        long total = 0;
        for (final Account account : accounts.values()) {
            total += account.tinyBarBalance;
        }
        return total;
    }

    /**
     * Compares the running state against an {@link AllAccountBalances} snapshot.
     *
     * <p>Every account listed in the snapshot must match the running state exactly:
     * <ul>
     *   <li>HBAR balance must equal {@code account.tinyBarBalance}.
     *   <li>Each {@link TokenUnitBalance} in the snapshot must equal the running state:
     *       <ul>
     *         <li>For fungible tokens: the balance field is compared against
     *             {@code account.fungibleBalances.getOrDefault(tokenNum, 0)}.
     *         <li>For NFTs: the balance field (count of NFTs held) is compared against
     *             {@code account.nftSerials.getOrDefault(tokenNum, emptySet()).size()}.
     *       </ul>
     *       Since a token type is either fungible or NFT (never both), only one of these will be
     *       non-zero; the actual value is the sum of both lookups.
     * </ul>
     *
     * <p>Accounts present in the running state but absent from the snapshot are not flagged –
     * the snapshot is authoritative only for the accounts it lists.
     *
     * @param balanceSnapshot the snapshot to compare against
     * @return {@code true} if every snapshot entry matches; {@code false} otherwise (mismatches
     *         are logged at WARNING level before returning)
     */
    public boolean compare(final AllAccountBalances balanceSnapshot) {
        boolean matches = true;
        for (final SingleAccountBalances snapshotEntry : balanceSnapshot.allAccounts()) {
            final long accountNum = snapshotEntry.accountIDOrThrow().accountNumOrThrow();
            final Account account = accounts.get(accountNum);

            // HBAR check
            final long expectedHbar = snapshotEntry.hbarBalance();
            final long actualHbar = account == null ? 0L : account.tinyBarBalance;
            if (expectedHbar != actualHbar) {
                LOGGER.warning(
                        "Account " + accountNum + " HBAR mismatch: expected=" + expectedHbar + " actual=" + actualHbar);
                matches = false;
            }

            // Token checks – each TokenUnitBalance applies to exactly one token type, which is
            // either fungible (balance = units) or NFT (balance = count of serial numbers held).
            for (final TokenUnitBalance tokenUnitBalance : snapshotEntry.tokenUnitBalances()) {
                final long tokenNum = tokenUnitBalance.tokenIdOrThrow().tokenNum();
                final long expectedBalance = tokenUnitBalance.balance();
                final long actualBalance;
                if (account == null) {
                    actualBalance = 0L;
                } else {
                    final long fungible = account.fungibleBalances.getOrDefault(tokenNum, 0L);
                    final int nftCount = account.nftSerials
                            .getOrDefault(tokenNum, Collections.emptySet())
                            .size();
                    actualBalance = fungible + nftCount;
                }
                if (expectedBalance != actualBalance) {
                    LOGGER.warning("Account " + accountNum + " token " + tokenNum + " mismatch: expected="
                            + expectedBalance + " actual=" + actualBalance);
                    matches = false;
                }
            }
        }
        return matches;
    }
}
