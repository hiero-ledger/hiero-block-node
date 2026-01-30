// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import java.util.HexFormat;
import java.io.DataInputStream;
import org.hiero.block.tools.states.utils.Utils;

public record SigInfo(long classVersion, long round, long memberId, byte[] hash, byte[] sig) {

    public static SigInfo copyFrom(DataInputStream inStream) throws IOException {
        var classVersion = inStream.readLong();
        var round = inStream.readLong();
        var memberId = inStream.readLong();
        var hash = Utils.readByteArray(inStream);
        var sig = Utils.readByteArray(inStream);
        return new SigInfo(classVersion, round, memberId, hash, sig);
    }

    @Override
    public String toString() {
        return "SigInfo[" + "classVersion="
                + classVersion + ", round="
                + round + ", memberId="
                + memberId + ", hash="
                + HexFormat.of().formatHex(hash) + ", sig="
                + HexFormat.of().formatHex(sig) + ']';
    }
}
