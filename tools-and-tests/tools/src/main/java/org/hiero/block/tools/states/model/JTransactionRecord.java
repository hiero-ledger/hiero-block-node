// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import org.hiero.block.tools.states.utils.FCDataInputStream;

public record JTransactionRecord(
        JTransactionReceipt txReceipt,
        byte[] txHash,
        JTransactionID transactionID,
        JTimestamp consensusTimestamp,
        String memo,
        long transactionFee,
        JContractFunctionResult contractCallResult,
        JContractFunctionResult contractCreateResult,
        JTransferList jTransferList,
        long expirationTime,
        // track deserialize version to ensure hash matches
        Long deserializedVersion) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long LEGACY_VERSION_2 = 2;
    private static final long CURRENT_VERSION = 3;

    public static JTransactionRecord copyFrom(final FCDataInputStream inStream) {
        try {
            JTransactionReceipt txReceipt;
            byte[] txHash;
            JTransactionID transactionID;
            JTimestamp consensusTimestamp;
            String memo;
            long transactionFee;
            JContractFunctionResult contractCallResult;
            JContractFunctionResult contractCreateResult;
            JTransferList jTransferList;
            long expirationTime = 0;
            // track deserialize version to ensure hash matches
            Long deserializedVersion = null;

            final long version = inStream.readLong();
            if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
                throw new IllegalStateException("Illegal version was read from the stream");
            }

            if (version != CURRENT_VERSION) {
                deserializedVersion = version;
            }

            final long objectType = inStream.readLong();
            final JObjectType type = JObjectType.valueOf(objectType);
            if (!JObjectType.JTransactionRecord.equals(type)) {
                throw new IllegalStateException("Illegal JObjectType was read from the stream");
            }

            boolean tBytes;
            if (version == LEGACY_VERSION_1) {
                tBytes = inStream.readInt() > 0;
            } else {
                tBytes = inStream.readBoolean();
            }

            if (tBytes) {
                txReceipt = JTransactionReceipt.copyFrom(inStream);
            } else {
                txReceipt = null;
            }

            byte[] hBytes = new byte[inStream.readInt()];
            if (hBytes.length > 0) {
                inStream.readFully(hBytes);
                txHash = hBytes;
            } else {
                txHash = null;
            }

            boolean txBytes;
            if (version == LEGACY_VERSION_1) {
                txBytes = inStream.readInt() > 0;
            } else {
                txBytes = inStream.readBoolean();
            }

            if (txBytes) {
                transactionID = JTransactionID.copyFrom(inStream);
            } else {
                transactionID = null;
            }

            boolean cBytes;
            if (version == LEGACY_VERSION_1) {
                cBytes = inStream.readInt() > 0;
            } else {
                cBytes = inStream.readBoolean();
            }

            if (cBytes) {
                consensusTimestamp = JTimestamp.copyFrom(inStream);
            } else {
                consensusTimestamp = null;
            }

            byte[] mBytes = new byte[inStream.readInt()];
            if (mBytes.length > 0) {
                inStream.readFully(mBytes);
                memo = new String(mBytes);
            } else {
                memo = null;
            }

            transactionFee = inStream.readLong();

            boolean trBytes;

            if (version == LEGACY_VERSION_1) {
                trBytes = inStream.readInt() > 0;
            } else {
                trBytes = inStream.readBoolean();
            }

            if (trBytes) {
                jTransferList = JTransferList.copyFrom(inStream);
            } else {
                jTransferList = null;
            }

            boolean clBytes;
            if (version == LEGACY_VERSION_1) {
                clBytes = inStream.readInt() > 0;
            } else {
                clBytes = inStream.readBoolean();
            }

            if (clBytes) {
                contractCallResult = JContractFunctionResult.deserialize(inStream);
            } else {
                contractCallResult = null;
            }

            boolean ccBytes;
            if (version == LEGACY_VERSION_1) {
                ccBytes = inStream.readInt() > 0;
            } else {
                ccBytes = inStream.readBoolean();
            }

            if (ccBytes) {
                contractCreateResult = JContractFunctionResult.deserialize(inStream);
            } else {
                contractCreateResult = null;
            }

            if (version >= CURRENT_VERSION) {
                expirationTime = inStream.readLong();
            }

            return new JTransactionRecord(
                    txReceipt,
                    txHash,
                    transactionID,
                    consensusTimestamp,
                    memo,
                    transactionFee,
                    contractCallResult,
                    contractCreateResult,
                    jTransferList,
                    expirationTime,
                    deserializedVersion);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy JTransactionRecord from input stream", e);
        }
    }
}
