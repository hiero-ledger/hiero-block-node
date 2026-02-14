// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Manual protobuf parser for AllAccountBalances that bypasses PBJ's size limits.
 * Parses the wire format directly and writes account balances to a DataOutputStream.
 *
 * <p>Wire format for AllAccountBalances:
 * <ul>
 *   <li>Field 1 (consensusTimestamp): Timestamp submessage - skipped</li>
 *   <li>Field 2 (allAccounts): repeated SingleAccountBalances</li>
 * </ul>
 *
 * <p>Wire format for SingleAccountBalances:
 * <ul>
 *   <li>Field 1 (accountID): AccountID submessage</li>
 *   <li>Field 2 (hbarBalance): uint64</li>
 *   <li>Field 3 (tokenUnitBalances): repeated - skipped</li>
 * </ul>
 */
@SuppressWarnings("CyclomaticComplexity")
public class BalanceProtobufParser {

    /** Holds parsed account balance data */
    private record AccountBalance(long accountNum, long balance) {}

    /**
     * Parse AllAccountBalances protobuf and return as a Map.
     *
     * @param pbBytes the protobuf bytes
     * @return map of account number to tinybar balance
     */
    public static Map<Long, Long> parseToMap(byte[] pbBytes) {
        Map<Long, Long> balances = new HashMap<>();
        ByteBuffer buf = ByteBuffer.wrap(pbBytes);

        while (buf.hasRemaining()) {
            int tag = readVarint32(buf);
            int fieldNum = tag >>> 3;
            int wireType = tag & 0x7;

            if (fieldNum == 2 && wireType == 2) {
                AccountBalance account = parseSingleAccountBalances(buf);
                if (account.accountNum > 0) {
                    balances.put(account.accountNum, account.balance);
                }
            } else {
                skipField(buf, wireType);
            }
        }

        return balances;
    }

    /**
     * Parse AllAccountBalances protobuf and write account balances to output.
     * Writes: accountCount (int) followed by [accountNum (long), balance (long)] pairs.
     *
     * @param pbBytes the protobuf bytes
     * @param out the output stream to write to
     * @return the number of accounts parsed
     * @throws IOException if parsing or writing fails
     */
    public static int parseAndWrite(byte[] pbBytes, DataOutputStream out) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(pbBytes);

        // First pass: count accounts
        int accountCount = countAccounts(pbBytes);

        // Write account count
        out.writeInt(accountCount);

        // Second pass: extract and write account data
        while (buf.hasRemaining()) {
            int tag = readVarint32(buf);
            int fieldNum = tag >>> 3;
            int wireType = tag & 0x7;

            if (fieldNum == 2 && wireType == 2) {
                AccountBalance account = parseSingleAccountBalances(buf);
                if (account.accountNum > 0) {
                    out.writeLong(account.accountNum);
                    out.writeLong(account.balance);
                }
            } else {
                skipField(buf, wireType);
            }
        }

        return accountCount;
    }

    /**
     * Count the number of account entries in the protobuf.
     */
    private static int countAccounts(byte[] pbBytes) {
        ByteBuffer buf = ByteBuffer.wrap(pbBytes);
        int count = 0;

        while (buf.hasRemaining()) {
            int tag = readVarint32(buf);
            int fieldNum = tag >>> 3;
            int wireType = tag & 0x7;

            if (fieldNum == 2 && wireType == 2) {
                count++;
                int len = readVarint32(buf);
                buf.position(buf.position() + len);
            } else {
                skipField(buf, wireType);
            }
        }

        return count;
    }

    /**
     * Parse a SingleAccountBalances submessage.
     */
    private static AccountBalance parseSingleAccountBalances(ByteBuffer buf) {
        int len = readVarint32(buf);
        int endPos = buf.position() + len;
        long accountNum = 0;
        long balance = 0;

        while (buf.position() < endPos) {
            int innerTag = readVarint32(buf);
            int innerFieldNum = innerTag >>> 3;
            int innerWireType = innerTag & 0x7;

            if (innerFieldNum == 1 && innerWireType == 2) {
                accountNum = parseAccountId(buf);
            } else if (innerFieldNum == 2 && innerWireType == 0) {
                balance = readVarint64(buf);
            } else {
                skipField(buf, innerWireType);
            }
        }
        buf.position(endPos);

        return new AccountBalance(accountNum, balance);
    }

    /**
     * Parse an AccountID submessage and return the account number.
     */
    private static long parseAccountId(ByteBuffer buf) {
        int idLen = readVarint32(buf);
        int idEndPos = buf.position() + idLen;
        long accountNum = 0;

        while (buf.position() < idEndPos) {
            int idTag = readVarint32(buf);
            int idFieldNum = idTag >>> 3;
            int idWireType = idTag & 0x7;

            if (idFieldNum == 3 && idWireType == 0) {
                accountNum = readVarint64(buf);
            } else {
                skipField(buf, idWireType);
            }
        }

        return accountNum;
    }

    private static int readVarint32(ByteBuffer buf) {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return result;
    }

    private static long readVarint64(ByteBuffer buf) {
        long result = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return result;
    }

    private static void skipField(ByteBuffer buf, int wireType) {
        switch (wireType) {
            case 0 -> readVarint64(buf); // varint
            case 1 -> buf.position(buf.position() + 8); // 64-bit
            case 2 -> { // length-delimited
                int len = readVarint32(buf);
                buf.position(buf.position() + len);
            }
            case 5 -> buf.position(buf.position() + 4); // 32-bit
            default -> throw new IllegalStateException("Unknown wire type: " + wireType);
        }
    }
}
