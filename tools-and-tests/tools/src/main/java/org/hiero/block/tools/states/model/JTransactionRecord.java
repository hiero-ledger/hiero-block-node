// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * A serializable transaction record containing receipt, hash, transfers, and contract results.
 *
 * @param txReceipt the transaction receipt, or {@code null}
 * @param txHash the transaction hash bytes, or {@code null}
 * @param transactionID the transaction identifier, or {@code null}
 * @param consensusTimestamp the consensus timestamp, or {@code null}
 * @param memo the transaction memo string, or {@code null}
 * @param transactionFee the transaction fee in tinybars
 * @param contractCallResult the result of a contract call, or {@code null}
 * @param contractCreateResult the result of contract creation, or {@code null}
 * @param jTransferList the list of HBAR transfers, or {@code null}
 * @param expirationTime the record expiration time in seconds since epoch
 * @param deserializedVersion the version this record was deserialized from, or {@code null} for current version
 */
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
        Long deserializedVersion) {
    /** The legacy serialization version identifier (version 1). */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version identifier (version 3). */
    private static final long CURRENT_VERSION = 3;

    /**
     * Deserializes a JTransactionRecord from the given input stream.
     *
     * @param inStream the input stream to read from
     * @return the deserialized JTransactionRecord instance
     */
    public static JTransactionRecord copyFrom(final DataInputStream inStream) {
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
