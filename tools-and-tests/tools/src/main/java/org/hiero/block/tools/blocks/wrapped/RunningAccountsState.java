// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import com.hedera.hapi.streams.AllAccountBalances;
import com.hedera.hapi.streams.SingleAccountBalances;
import com.hedera.hapi.streams.TokenUnitBalance;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

/**
 * Tracks running account balances (HBAR, fungible tokens, and NFTs) across a sequence of wrapped
 * blocks.
 *
 * <p>This class uses Eclipse Collections primitive maps and sets to avoid autoboxing overhead
 * when tracking millions of accounts across millions of blocks. The main data structure is a
 * {@code LongObjectHashMap<Account>} keyed by account number, where each {@link Account} holds:
 * <ul>
 *   <li>The tinybar HBAR balance.
 *   <li>Fungible-token balances: {@code LongLongHashMap} (tokenNum → units).
 *   <li>NFT ownership: {@code LongObjectHashMap<LongHashSet>} (tokenNum → set of serial numbers).
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
 * 50-billion-HBAR supply check), {@link #getHbarBalance(long)} to query a single account,
 * and {@link #compare(AllAccountBalances)} to validate the running state against a balance
 * snapshot read from disk.
 */
public class RunningAccountsState {

    private static final Logger LOGGER = Logger.getLogger(RunningAccountsState.class.getName());

    /** Per-account state: HBAR balance, fungible token units, and owned NFT serial numbers. */
    private static class Account {
        long tinyBarBalance = 0;
        /** tokenNum → fungible units held. */
        final LongLongHashMap fungibleBalances = new LongLongHashMap();
        /** tokenNum → set of NFT serial numbers owned. */
        final LongObjectHashMap<LongHashSet> nftSerials = new LongObjectHashMap<>();
    }

    private final LongObjectHashMap<Account> accounts = new LongObjectHashMap<>();

    /** Cached sum of all HBAR balances, updated incrementally to avoid iterating all accounts. */
    private long runningHbarTotal = 0;

    /**
     * Sets the HBAR balance for an account to an absolute value (from StateChanges map update).
     * Creates the account entry if it does not yet exist.
     *
     * @param accountNum     the account number
     * @param tinyBarBalance the new absolute tinybar balance
     */
    public void setHbarBalance(final long accountNum, final long tinyBarBalance) {
        final Account account = accounts.getIfAbsentPut(accountNum, Account::new);
        runningHbarTotal += (tinyBarBalance - account.tinyBarBalance);
        account.tinyBarBalance = tinyBarBalance;
    }

    /**
     * Applies a relative HBAR balance change to an account (from RecordFile transfer list).
     * Creates the account entry if it does not yet exist.
     *
     * @param accountNum    the account number
     * @param tinyBarChange the signed delta in tinybar (positive = credit, negative = debit)
     */
    public void applyHbarChange(final long accountNum, final long tinyBarChange) {
        accounts.getIfAbsentPut(accountNum, Account::new).tinyBarBalance += tinyBarChange;
        runningHbarTotal += tinyBarChange;
    }

    /**
     * Removes an account entry (from StateChanges map delete).
     *
     * @param accountIdNum the account number
     */
    public void deleteAccount(final long accountIdNum) {
        final Account removed = accounts.remove(accountIdNum);
        if (removed != null) {
            runningHbarTotal -= removed.tinyBarBalance;
        }
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
        final Account account = accounts.getIfAbsentPut(accountNum, Account::new);
        account.fungibleBalances.addToValue(tokenNum, amount);
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
        final Account sender = accounts.getIfAbsentPut(senderAccountNum, Account::new);
        sender.nftSerials.getIfAbsentPut(tokenNum, LongHashSet::new).remove(serialNumber);

        final Account receiver = accounts.getIfAbsentPut(receiverAccountNum, Account::new);
        receiver.nftSerials.getIfAbsentPut(tokenNum, LongHashSet::new).add(serialNumber);
    }

    /**
     * Returns the HBAR balance for a single account. Returns 0 if the account does not exist.
     *
     * @param accountNum the account number
     * @return the tinybar balance, or 0 if the account is not tracked
     */
    public long getHbarBalance(final long accountNum) {
        final Account account = accounts.get(accountNum);
        return account == null ? 0L : account.tinyBarBalance;
    }

    /**
     * Returns the sum of all HBAR balances across all tracked accounts.
     * This is maintained incrementally and returns in O(1) time.
     *
     * @return total tinybar balance across all accounts
     */
    public long totalHbarBalance() {
        return runningHbarTotal;
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
     *             {@code account.fungibleBalances.getIfAbsent(tokenNum, 0)}.
     *         <li>For NFTs: the balance field (count of NFTs held) is compared against
     *             {@code account.nftSerials.get(tokenNum).size()}.
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
                    final long fungible = account.fungibleBalances.getIfAbsent(tokenNum, 0L);
                    final LongHashSet nftSet = account.nftSerials.get(tokenNum);
                    final int nftCount = nftSet == null ? 0 : nftSet.size();
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

    /**
     * Returns a map of all HBAR balances (account number to tinybar balance).
     * Used for checkpoint validation. Creates boxed copies since checkpoints are infrequent.
     *
     * @return map of account number to tinybar balance
     */
    public Map<Long, Long> getHbarBalances() {
        final Map<Long, Long> result = new HashMap<>();
        accounts.forEachKeyValue((accountNum, account) -> result.put(accountNum, account.tinyBarBalance));
        return result;
    }

    /**
     * Returns a map of all token balances (account number to token number to balance).
     * This includes both fungible token balances and NFT counts (number of serial numbers held).
     * Used for checkpoint validation. Creates boxed copies since checkpoints are infrequent.
     *
     * @return map of account number to (token number to balance)
     */
    public Map<Long, Map<Long, Long>> getTokenBalances() {
        final Map<Long, Map<Long, Long>> result = new HashMap<>();
        accounts.forEachKeyValue((accountNum, account) -> {
            final Map<Long, Long> tokenBalances = new HashMap<>();

            // Add fungible balances
            account.fungibleBalances.forEachKeyValue(tokenBalances::put);

            // Add NFT counts
            account.nftSerials.forEachKeyValue(
                    (tokenNum, serials) -> tokenBalances.merge(tokenNum, (long) serials.size(), Long::sum));

            if (!tokenBalances.isEmpty()) {
                result.put(accountNum, tokenBalances);
            }
        });
        return result;
    }

    /**
     * Saves the full account state to a binary file for checkpoint persistence.
     *
     * <p>Binary format (DataOutputStream):
     * <ol>
     *   <li>{@code int} version = 1
     *   <li>{@code int} accountCount
     *   <li>For each account:
     *     <ul>
     *       <li>{@code long} accountNum
     *       <li>{@code long} tinyBarBalance
     *       <li>{@code int} fungibleTokenCount
     *       <li>For each fungible token: {@code long} tokenNum, {@code long} balance
     *       <li>{@code int} nftTokenCount
     *       <li>For each NFT token: {@code long} tokenNum, {@code int} serialCount,
     *           then each {@code long} serialNumber
     *     </ul>
     * </ol>
     *
     * @param path the file path to write to
     * @throws IOException if writing fails
     */
    public void save(final Path path) throws IOException {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
            out.writeInt(1); // version
            out.writeInt(accounts.size());
            final long[] accountKeys = accounts.keysView().toArray();
            for (final long accountNum : accountKeys) {
                final Account account = accounts.get(accountNum);
                out.writeLong(accountNum);
                out.writeLong(account.tinyBarBalance);
                // Fungible tokens
                final long[] fungibleKeys = account.fungibleBalances.keysView().toArray();
                out.writeInt(fungibleKeys.length);
                for (final long tokenNum : fungibleKeys) {
                    out.writeLong(tokenNum);
                    out.writeLong(account.fungibleBalances.get(tokenNum));
                }
                // NFT tokens
                final long[] nftTokenKeys = account.nftSerials.keysView().toArray();
                out.writeInt(nftTokenKeys.length);
                for (final long tokenNum : nftTokenKeys) {
                    final LongHashSet serials = account.nftSerials.get(tokenNum);
                    out.writeLong(tokenNum);
                    final long[] serialArray = serials.toArray();
                    out.writeInt(serialArray.length);
                    for (final long serial : serialArray) {
                        out.writeLong(serial);
                    }
                }
            }
        }
    }

    /**
     * Loads account state from a binary file, replacing any existing state.
     *
     * @param path the file path to read from
     * @throws IOException if reading fails
     * @see #save(Path)
     */
    public void load(final Path path) throws IOException {
        accounts.clear();
        runningHbarTotal = 0;
        try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
            final int version = in.readInt();
            if (version != 1) {
                throw new IOException("Unknown RunningAccountsState version: " + version);
            }
            final int accountCount = in.readInt();
            for (int i = 0; i < accountCount; i++) {
                final long accountNum = in.readLong();
                final Account account = new Account();
                account.tinyBarBalance = in.readLong();
                runningHbarTotal += account.tinyBarBalance;
                // Fungible tokens
                final int fungibleCount = in.readInt();
                for (int j = 0; j < fungibleCount; j++) {
                    account.fungibleBalances.put(in.readLong(), in.readLong());
                }
                // NFT tokens
                final int nftCount = in.readInt();
                for (int j = 0; j < nftCount; j++) {
                    final long tokenNum = in.readLong();
                    final int serialCount = in.readInt();
                    final LongHashSet serials = new LongHashSet(serialCount);
                    for (int k = 0; k < serialCount; k++) {
                        serials.add(in.readLong());
                    }
                    account.nftSerials.put(tokenNum, serials);
                }
                accounts.put(accountNum, account);
            }
        }
    }
}
