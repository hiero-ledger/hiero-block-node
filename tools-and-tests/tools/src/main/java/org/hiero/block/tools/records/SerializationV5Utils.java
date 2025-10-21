// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;

import com.hedera.pbj.runtime.io.ReadableSequentialData;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Utility methods for serialization and deserialization of record stream objects.
 */
@SuppressWarnings("DuplicatedCode")
public class SerializationV5Utils {
    /** The size of a hash object in bytes */
    public static final int HASH_OBJECT_SIZE_BYTES =
            Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES + SHA_384_HASH_SIZE;
    /** The class ID for the hash object */
    private static final long HASH_CLASS_ID = 0xf422da83a251741eL;
    /** The class version for the hash object */
    private static final int HASH_CLASS_VERSION = 1;
    /** The digest type for SHA384 */
    private static final int DIGEST_TYPE_SHA384 = 0x58ff811b;

    /**
     * Read a record stream v5 hash object from a data input stream in SelfSerializable SHA384 format.
     *
     * @param in the data input stream
     * @return the hash bytes
     * @throws IOException if an error occurs reading the hash object
     */
    public static byte[] readV5HashObject(DataInputStream in) throws IOException {
        // read hash class id
        final long classId = in.readLong();
        if (classId != HASH_CLASS_ID) {
            throw new IllegalArgumentException(
                    "Invalid hash class ID [" + Long.toHexString(classId) + "] expected [f422da83a251741e]");
        }
        // read hash class version
        final int classVersion = in.readInt();
        if (classVersion != HASH_CLASS_VERSION) {
            throw new IllegalArgumentException("Invalid hash class version [" + classVersion + "] expected [1]");
        }
        // read hash object, starting with digest type SHA384
        final int digestType = in.readInt();
        if (digestType != DIGEST_TYPE_SHA384) {
            throw new IllegalArgumentException(
                    "Invalid digest type not SHA384 [" + Integer.toHexString(digestType) + "] expected [58ff811b]");
        }
        // read hash object - length of hash
        final int hashLength = in.readInt();
        if (hashLength != 48) {
            throw new IllegalArgumentException("Invalid hash length [" + hashLength + "] expected [48]");
        }
        // read hash object - hash bytes
        final byte[] entireFileHash = new byte[SHA_384_HASH_SIZE];
        in.readFully(entireFileHash);
        return entireFileHash;
    }

    /**
     * Read a record stream v5 hash object from a data input stream in SelfSerializable SHA384 format.
     *
     * @param in the data input stream
     * @return the hash bytes
     * @throws IOException if an error occurs reading the hash object
     */
    public static byte[] readV5HashObject(ReadableSequentialData in) throws IOException {
        // read hash class id
        final long classId = in.readLong();
        if (classId != HASH_CLASS_ID) {
            throw new IllegalArgumentException(
                    "Invalid hash class ID [" + Long.toHexString(classId) + "] expected [f422da83a251741e]");
        }
        // read hash class version
        final int classVersion = in.readInt();
        if (classVersion != HASH_CLASS_VERSION) {
            throw new IllegalArgumentException("Invalid hash class version [" + classVersion + "] expected [1]");
        }
        // read hash object, starting with digest type SHA384
        final int digestType = in.readInt();
        if (digestType != DIGEST_TYPE_SHA384) {
            throw new IllegalArgumentException(
                    "Invalid digest type not SHA384 [" + Integer.toHexString(digestType) + "] expected [58ff811b]");
        }
        // read hash object - length of hash
        final int hashLength = in.readInt();
        if (hashLength != 48) {
            throw new IllegalArgumentException("Invalid hash length [" + hashLength + "] expected [48]");
        }
        // read hash object - hash bytes
        final byte[] entireFileHash = new byte[SHA_384_HASH_SIZE];
        in.readBytes(entireFileHash);
        return entireFileHash;
    }
}
