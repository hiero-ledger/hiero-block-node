package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public record JContractFunctionResult(JAccountID contractID, byte[] result, String error, byte[] bloom,
                                      long gasUsed, List<JContractLogInfo>jContractLogInfo) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;


    public static JContractFunctionResult deserialize(final FCDataInputStream inStream) throws IOException {
        final long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        final long objectType = inStream.readLong();
        final JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JContractFunctionResult.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        JAccountID contractID;
        byte[] result;
        String error;
        byte[] bloom;
        long gasUsed;
        List<JContractLogInfo>jContractLogInfo;

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

        final byte[] RBytes = new byte[inStream.readInt()];
        if (RBytes.length > 0) {
            inStream.readFully(RBytes);
            result = RBytes;
        } else {
            result = null;
        }

        final byte[] eBytes = new byte[inStream.readInt()];
        if (eBytes.length > 0) {
            inStream.readFully(eBytes);
            error = new String(eBytes);
        } else {
            error = null;
        }

        final byte[] BBytes = new byte[inStream.readInt()];
        if (BBytes.length > 0) {
            inStream.readFully(BBytes);
            bloom = BBytes;
        } else {
            bloom = null;
        }

        gasUsed = inStream.readLong();

        final int listSize = inStream.readInt();
        if (listSize > 0) {
            final List<JContractLogInfo> list = new ArrayList<>();
            for (int i = 0; i < listSize; i++) {
                list.add(JContractLogInfo.copyFrom(inStream));
            }
            jContractLogInfo = list;
        } else {
            jContractLogInfo = new LinkedList<>();
        }
        return new JContractFunctionResult(contractID, result, error, bloom, gasUsed, jContractLogInfo);
    }
}
