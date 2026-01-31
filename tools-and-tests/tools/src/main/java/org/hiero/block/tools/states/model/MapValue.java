// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A serializable value for the account FCMap, containing all account properties.
 *
 * @param balance the account balance in tinybars
 * @param receiverThreshold the threshold amount in tinybars for incoming transfers
 * @param senderThreshold the threshold amount in tinybars for outgoing transfers
 * @param receiverSigRequired whether the receiver signature is required for incoming transfers
 * @param accountKeys the cryptographic keys associated with this account
 * @param proxyAccount the proxy account ID for staking, or {@code null} if none
 * @param autoRenewPeriod the auto-renewal period in seconds
 * @param deleted whether the account has been deleted
 * @param recordLinkedList the linked list of transaction records associated with this account
 * @param expirationTime the account expiration time in seconds since epoch
 * @param memo the account memo string
 * @param isSmartContract whether this account represents a smart contract
 */
@SuppressWarnings({"ConstantValue", "DuplicatedCode"})
public record MapValue(
        long balance,
        long receiverThreshold,
        long senderThreshold,
        boolean receiverSigRequired,
        JKey accountKeys,
        JAccountID proxyAccount,
        long autoRenewPeriod,
        boolean deleted,
        FCLinkedList<JTransactionRecord> recordLinkedList,
        long expirationTime,
        String memo,
        boolean isSmartContract) {
    /** The first legacy serialization version identifier (version 1). */
    private static final long LEGACY_VERSION_1 = 1;
    /** The second legacy serialization version identifier (version 2). */
    private static final long LEGACY_VERSION_2 = 2;
    /** The third legacy serialization version identifier (version 3). */
    private static final long LEGACY_VERSION_3 = 3;
    /** The current serialization version identifier (version 4). */
    private static final long CURRENT_VERSION = 4;
    /** The unique object identifier for this type. */
    private static final long OBJECT_ID = 15487001;

    /**
     * Deserializes a MapValue from the given input stream.
     *
     * @param inStream the input stream to read from
     * @return the deserialized MapValue instance
     * @throws IOException if an I/O error occurs or version/object ID mismatch is detected
     */
    public static MapValue copyFrom(DataInputStream inStream) throws IOException {
        long balance = 0L;
        long receiverThreshold = 0;
        long senderThreshold = 0;
        boolean receiverSigRequired = false;
        JKey accountKeys = null;
        JAccountID proxyAccount = null;
        long autoRenewPeriod = 0;
        boolean deleted = false;
        FCLinkedList<JTransactionRecord> recordLinkedList = new FCLinkedList<>();
        long expirationTime = 0;
        String memo = null;
        boolean isSmartContract = false;

        long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IOException("Unsupported MapValue version: " + version);
        }
        long objectId = inStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected MapValue object ID: " + objectId);
        }

        if (version == LEGACY_VERSION_1) {
            balance = inStream.readLong();
            senderThreshold = inStream.readLong();
            receiverThreshold = inStream.readLong();
            receiverSigRequired = inStream.readChar() == 1;

            byte[] ethAddressByte = new byte[20];
            inStream.readFully(ethAddressByte);
            accountKeys = JKey.copyFrom(inStream);
            if (accountKeys != null && accountKeys.hasContractID()) {
                isSmartContract = true;
            }
        } else if (version == LEGACY_VERSION_2) {
            balance = inStream.readLong();
            senderThreshold = inStream.readLong();
            receiverThreshold = inStream.readLong();
            receiverSigRequired = inStream.readChar() == 1;
            accountKeys = JKey.copyFrom(inStream);

            if (inStream.readChar() == ApplicationConstants.P) {
                proxyAccount = JAccountID.copyFrom(inStream);
            }
            autoRenewPeriod = inStream.readLong();
            deleted = inStream.readChar() == 1;
            if (accountKeys != null && accountKeys.hasContractID()) {
                isSmartContract = true;
            }
        } else if (version == LEGACY_VERSION_3) {
            balance = inStream.readLong();
            senderThreshold = inStream.readLong();
            receiverThreshold = inStream.readLong();
            receiverSigRequired = inStream.readByte() == 1;
            accountKeys = JKey.copyFrom(inStream);
            if (inStream.readChar() == ApplicationConstants.P) {
                proxyAccount = JAccountID.copyFrom(inStream);
            }
            autoRenewPeriod = inStream.readLong();
            deleted = inStream.readByte() == 1;
            expirationTime = inStream.readLong();
            memo = "";
            if (accountKeys != null && accountKeys.hasContractID()) {
                isSmartContract = true;
            }
            recordLinkedList = FCLinkedList.copyFrom(inStream, JTransactionRecord::copyFrom);
        } else if (version == CURRENT_VERSION) {
            balance = inStream.readLong();
            senderThreshold = inStream.readLong();
            receiverThreshold = inStream.readLong();
            receiverSigRequired = inStream.readByte() == 1;
            accountKeys = JKey.copyFrom(inStream);
            if (inStream.readChar() == ApplicationConstants.P) {
                proxyAccount = JAccountID.copyFrom(inStream);
            }
            autoRenewPeriod = inStream.readLong();
            deleted = inStream.readByte() == 1;
            expirationTime = inStream.readLong();
            memo = inStream.readUTF();
            isSmartContract = inStream.readByte() == 1;
            recordLinkedList = FCLinkedList.copyFrom(inStream, JTransactionRecord::copyFrom);
        }
        return new MapValue(
                balance,
                receiverThreshold,
                senderThreshold,
                receiverSigRequired,
                accountKeys,
                proxyAccount,
                autoRenewPeriod,
                deleted,
                recordLinkedList,
                expirationTime,
                memo,
                isSmartContract);
    }

    /**
     * Serializes this MapValue to the given output stream, matching the original serialize() format.
     *
     * @param out the output stream to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    public void copyTo(DataOutputStream out) throws IOException {
        out.writeLong(CURRENT_VERSION);
        out.writeLong(OBJECT_ID);
        out.writeLong(balance);
        out.writeLong(senderThreshold);
        out.writeLong(receiverThreshold);
        out.writeByte(receiverSigRequired ? 1 : 0);
        out.write(accountKeys.serialize());
        if (proxyAccount != null) {
            out.writeChar(ApplicationConstants.P);
            proxyAccount.copyTo(out);
        } else {
            out.writeChar(ApplicationConstants.N);
        }
        out.writeLong(autoRenewPeriod);
        out.writeByte(deleted ? 1 : 0);
        out.writeLong(expirationTime);
        out.writeUTF(memo == null ? "" : memo);
        out.writeByte(isSmartContract ? 1 : 0);
        recordLinkedList.copyTo(out);
    }
}
