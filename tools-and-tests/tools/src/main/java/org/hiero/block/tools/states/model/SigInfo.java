package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;
import org.hiero.block.tools.states.utils.Utilities;
import java.io.IOException;
import java.util.HexFormat;

public record SigInfo(
        long classVersion,
        long round,
        long memberId,
        byte[] hash,
        byte[] sig
) {


    public static SigInfo copyFrom(FCDataInputStream inStream) throws IOException {
        var classVersion = inStream.readLong();
        var round = inStream.readLong();
        var memberId = inStream.readLong();
        var hash = Utilities.readByteArray(inStream);
        var sig = Utilities.readByteArray(inStream);
        return new SigInfo(classVersion, round, memberId, hash, sig);
    }

    @Override
    public String toString() {
        return "SigInfo[" +
                "classVersion=" + classVersion +
                ", round=" + round +
                ", memberId=" + memberId +
                ", hash=" + HexFormat.of().formatHex(hash) +
                ", sig=" + HexFormat.of().formatHex(sig) +
                ']';
    }
}
