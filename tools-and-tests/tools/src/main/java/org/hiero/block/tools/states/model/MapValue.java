// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** A serializable value for the account FCMap, containing all account properties. */
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
    private static final long LEGACY_VERSION_1 = 1;
    private static final long LEGACY_VERSION_2 = 2;
    private static final long LEGACY_VERSION_3 = 3;
    private static final long CURRENT_VERSION = 4;
    private static final long OBJECT_ID = 15487001;

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

    /** Serializes this MapValue matching the original serialize() + recordLinkedList.copyTo(). */
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
