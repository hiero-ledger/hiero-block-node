// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * A serializable smart contract log entry with bloom filter, data, and topics.
 *
 * @param contractID the contract identifier that emitted this log, or {@code null}
 * @param bloom the bloom filter bytes for this log entry, or {@code null}
 * @param data the log data bytes, or {@code null}
 * @param topic the list of indexed topic byte arrays for event filtering
 */
public record JContractLogInfo(JAccountID contractID, byte[] bloom, byte[] data, List<byte[]> topic) {
    /** The legacy serialization version identifier (version 1). */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version identifier (version 2). */
    private static final long CURRENT_VERSION = 2;

    /**
     * Deserializes a JContractLogInfo from the given input stream.
     *
     * @param inStream the input stream to read from
     * @return the deserialized JContractLogInfo instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static JContractLogInfo copyFrom(DataInputStream inStream) throws IOException {
        JAccountID contractID;
        byte[] bloom;
        byte[] data;
        List<byte[]> topic;

        final long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        final long objectType = inStream.readLong();
        final JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JContractLogInfo.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        boolean contractIDPresent;

        if (version == LEGACY_VERSION_1) {
            contractIDPresent = inStream.readInt() > 0;
        } else {
            contractIDPresent = inStream.readBoolean();
        }

        if (contractIDPresent) {
            contractID = JAccountID.copyFrom(inStream);
        } else {
            contractID = null;
        }

        final byte[] BBytes = new byte[inStream.readInt()];
        if (BBytes.length > 0) {
            inStream.readFully(BBytes);
            bloom = BBytes;
        } else {
            bloom = null;
        }

        final byte[] DBytes = new byte[inStream.readInt()];
        if (DBytes.length > 0) {
            inStream.readFully(DBytes);
            data = DBytes;
        } else {
            data = null;
        }

        final int listSize = inStream.readInt();
        if (listSize > 0) {
            List<byte[]> topicList = new LinkedList<>();
            for (int i = 0; i < listSize; i++) {
                byte[] TBytes = new byte[inStream.readInt()];

                if (TBytes.length > 0) {
                    inStream.readFully(TBytes);
                }

                topicList.add(TBytes);
            }

            topic = topicList;
        } else {
            topic = new LinkedList<>();
        }
        return new JContractLogInfo(contractID, bloom, data, topic);
    }
}
