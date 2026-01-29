package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.FCDataInputStream;
import java.io.IOException;
import java.util.List;

public record JTransferList(List<JAccountAmount> jAccountAmountsList) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;


    public static JTransferList copyFrom(final FCDataInputStream inStream)
            throws IOException {
        final long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        final long objectType = inStream.readLong();
        final JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JTransferList.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }
        List<JAccountAmount> jAccountAmountsList = new java.util.ArrayList<>();
        int listSize = inStream.readInt();
        for (int i = 0; i < listSize; i++) {
            jAccountAmountsList.add(JAccountAmount.copyFrom(inStream));
        }

        return new JTransferList(jAccountAmountsList);
    }
}
