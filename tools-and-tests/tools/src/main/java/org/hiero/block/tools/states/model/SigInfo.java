// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HexFormat;
import org.hiero.block.tools.states.utils.Utils;
import org.jspecify.annotations.NonNull;

/**
 * A serializable signature info record containing round, member ID, hash, and signature bytes.
 *
 * @param classVersion the class version for serialization
 * @param round the round number for which this signature applies
 * @param memberId the ID of the member who created this signature
 * @param hash the hash bytes that were signed
 * @param sig the signature bytes
 */
public record SigInfo(long classVersion, long round, long memberId, byte[] hash, byte[] sig) {

    /**
     * Deserializes a SigInfo from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized SigInfo
     * @throws IOException if an I/O error occurs
     */
    public static SigInfo copyFrom(DataInputStream inStream) throws IOException {
        var classVersion = inStream.readLong();
        var round = inStream.readLong();
        var memberId = inStream.readLong();
        var hash = Utils.readByteArray(inStream);
        var sig = Utils.readByteArray(inStream);
        return new SigInfo(classVersion, round, memberId, hash, sig);
    }

    @Override
    public @NonNull String toString() {
        return "SigInfo[" + "classVersion="
                + classVersion + ", round="
                + round + ", memberId="
                + memberId + ", hash="
                + HexFormat.of().formatHex(hash) + ", sig="
                + HexFormat.of().formatHex(sig) + ']';
    }
}
