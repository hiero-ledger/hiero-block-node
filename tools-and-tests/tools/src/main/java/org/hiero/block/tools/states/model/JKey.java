// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;

/** Base class for serializable Hedera keys with BPACK format support and multiple key type subtypes. */
public class JKey {
    /** Legacy serialization version. */
    private static final long LEGACY_VERSION = 1;
    /** BPACK serialization version. */
    private static final long BPACK_VERSION = 2;

    /** BPACK version used for writing. */
    private static final long BPACK_VERSION_WRITE = 2;

    /**
     * Returns whether this key references a smart contract.
     *
     * @return {@code true} if this is a contract ID key
     */
    public boolean hasContractID() {
        return false;
    }

    /**
     * Serializes this JKey in the BPACK format matching the original JKeySerializer.serialize().
     * Format: version(8) + objectType(8) + contentLength(8) + content(variable)
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
        DataOutputStream contentDos = new DataOutputStream(contentBos);
        JObjectType type = packTo(contentDos);
        contentDos.flush();
        byte[] content = contentBos.toByteArray();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(BPACK_VERSION_WRITE);
        dos.writeLong(type.longValue());
        dos.writeLong(content.length);
        dos.write(content);
        dos.flush();
        return bos.toByteArray();
    }

    /** Packs the key-specific content and returns the JObjectType. Subclasses override. */
    protected JObjectType packTo(DataOutputStream out) throws IOException {
        throw new UnsupportedOperationException("Base JKey cannot be serialized");
    }

    /**
     * Returns the Ed25519 public key bytes, or {@code null} if not an Ed25519 key.
     *
     * @return the key bytes or {@code null}
     */
    public byte[] getEd25519() {
        return null;
    }

    /**
     * Returns the ECDSA secp384r1 public key bytes, or {@code null} if not an ECDSA-384 key.
     *
     * @return the key bytes or {@code null}
     */
    public byte[] getECDSA384() {
        return null;
    }

    /**
     * Returns the RSA-3072 public key bytes, or {@code null} if not an RSA-3072 key.
     *
     * @return the key bytes or {@code null}
     */
    public byte[] getRSA3072() {
        return null;
    }

    /**
     * Deserializes a JKey from the given stream.
     *
     * @param stream the stream to read from
     * @param <T> the expected key subtype
     * @return the deserialized key
     * @throws IOException if an I/O error occurs or the format is invalid
     */
    public static <T extends JKey> T copyFrom(DataInputStream stream) throws IOException {
        long version = stream.readLong();
        if (version < LEGACY_VERSION || version > BPACK_VERSION) {
            throw new IOException("Unsupported JKey version: " + version);
        }
        long objectType = stream.readLong();
        long length = stream.readLong();

        if (version == LEGACY_VERSION) {
            byte[] content = new byte[(int) length];
            return deserialize(content);
        }

        JObjectType type = JObjectType.valueOf(objectType);

        if (objectType < 0 || type == null) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        return unpack(stream, type, length);
    }

    /**
     * Deserializes an object from a byte array using Java serialization.
     *
     * @param objectData the serialized bytes
     * @param <T> the expected type
     * @return the deserialized object
     */
    public static <T> T deserialize(byte[] objectData) {
        return deserialize(new ByteArrayInputStream(objectData));
    }

    /**
     * Deserializes an object from an input stream using Java serialization.
     *
     * @param inputStream the stream to read from
     * @param <T> the expected type
     * @return the deserialized object
     */
    public static <T> T deserialize(InputStream inputStream) {

        try {
            ObjectInputStream in = new ObjectInputStream(inputStream);

            Object var3;
            try {
                @SuppressWarnings("unchecked")
                T obj = (T) in.readObject();
                var3 = obj;
            } catch (Throwable var5) {
                try {
                    in.close();
                } catch (Throwable var4) {
                    var5.addSuppressed(var4);
                }

                throw var5;
            }

            in.close();
            //noinspection unchecked
            return (T) var3;
        } catch (IOException | ClassNotFoundException ex) {
            throw new IllegalStateException("Failed to deserialize JKey", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends JKey> T unpack(DataInputStream stream, JObjectType type, long length) throws IOException {

        if (JObjectType.JEd25519Key.equals(type) || JObjectType.JECDSA_384Key.equals(type)) {
            byte[] key = new byte[(int) length];
            stream.readFully(key);

            return (JObjectType.JEd25519Key.equals(type)) ? (T) new JEd25519Key(key) : (T) new JECDSA_384Key(key);
        } else if (JObjectType.JThresholdKey.equals(type)) {
            int threshold = stream.readInt();
            JKeyList keyList = copyFrom(stream);

            return (T) new JThresholdKey(keyList, threshold);
        } else if (JObjectType.JKeyList.equals(type)) {
            List<JKey> elements = new LinkedList<>();

            int size = stream.readInt();

            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    elements.add(copyFrom(stream));
                }
            }

            return (T) new JKeyList(elements);
        } else if (JObjectType.JRSA_3072Key.equals(type)) {
            byte[] key = new byte[(int) length];
            stream.readFully(key);

            return (T) new JRSA_3072Key(key);
        } else if (JObjectType.JContractIDKey.equals(type)) {
            long shard = stream.readLong();
            long realm = stream.readLong();
            long contract = stream.readLong();

            return (T) new JContractIDKey(shard, realm, contract);
        } else {
            throw new IllegalStateException("Unknown type was encountered while reading from the input stream");
        }
    }

    /** An Ed25519 public key. */
    public static class JEd25519Key extends JKey {
        /** The Ed25519 public key bytes. */
        private byte[] ed25519 = null;

        /**
         * Creates an Ed25519 key.
         *
         * @param ed25519 the Ed25519 public key bytes
         */
        public JEd25519Key(byte[] ed25519) {
            this.ed25519 = ed25519;
        }

        @Override
        protected JObjectType packTo(DataOutputStream out) throws IOException {
            out.write(ed25519);
            return JObjectType.JEd25519Key;
        }

        @Override
        public String toString() {
            return "<JEd25519Key: ed25519 hex=" + HexFormat.of().formatHex(ed25519) + ">";
        }

        /**
         * Returns the Ed25519 public key bytes.
         *
         * @return the key bytes
         */
        public byte[] getEd25519() {
            return ed25519;
        }

        /**
         * Returns whether this key is an Ed25519 key.
         *
         * @return {@code true}
         */
        public boolean hasEd25519Key() {
            return true;
        }
    }

    /** An ordered list of keys. */
    public static class JKeyList extends JKey {

        /** The ordered list of keys. */
        private final List<JKey> keys;

        /**
         * Creates a key list.
         *
         * @param keys the ordered list of keys
         */
        public JKeyList(List<JKey> keys) {
            this.keys = keys;
        }

        @Override
        protected JObjectType packTo(DataOutputStream out) throws IOException {
            out.writeInt(keys.size());
            for (JKey key : keys) {
                out.write(key.serialize());
            }
            return JObjectType.JKeyList;
        }

        @Override
        public String toString() {
            return "<JKeyList: keys=" + keys.toString() + ">";
        }

        /**
         * Returns the ordered list of keys.
         *
         * @return the key list
         */
        public List<JKey> getKeysList() {
            return keys;
        }
    }

    /** An RSA-3072 public key. */
    public static class JRSA_3072Key extends JKey {

        /** The RSA-3072 public key bytes. */
        private final byte[] RSA_3072Key;

        /**
         * Creates an RSA-3072 key.
         *
         * @param RSA_3072Key the RSA-3072 public key bytes
         */
        public JRSA_3072Key(byte[] RSA_3072Key) {
            this.RSA_3072Key = RSA_3072Key;
        }

        @Override
        protected JObjectType packTo(DataOutputStream out) throws IOException {
            out.write(RSA_3072Key);
            return JObjectType.JRSA_3072Key;
        }

        @Override
        public String toString() {
            return "<JRSA_3072Key: RSA_3072Key hex=" + HexFormat.of().formatHex(RSA_3072Key) + ">";
        }

        /**
         * Returns the RSA-3072 public key bytes.
         *
         * @return the key bytes
         */
        public byte[] getRSA3072() {
            return RSA_3072Key;
        }
    }

    /** A key referencing a smart contract by its shard, realm, and contract number. */
    public static class JContractIDKey extends JKey {
        /** The shard number (nonnegative). */
        private final long shardNum;
        /** The realm number (nonnegative). */
        private final long realmNum;
        /** A nonnegative number unique within its realm. */
        private final long contractNum;

        /**
         * Returns whether this key references a smart contract.
         *
         * @return {@code true}
         */
        public boolean hasContractID() {
            return true;
        }

        /**
         * Creates a contract ID key.
         *
         * @param shardNum the shard number (nonnegative)
         * @param realmNum the realm number (nonnegative)
         * @param contractNum the contract number (nonnegative, unique within its realm)
         */
        public JContractIDKey(long shardNum, long realmNum, long contractNum) {
            super();
            this.shardNum = shardNum;
            this.realmNum = realmNum;
            this.contractNum = contractNum;
        }

        @Override
        protected JObjectType packTo(DataOutputStream out) throws IOException {
            out.writeLong(shardNum);
            out.writeLong(realmNum);
            out.writeLong(contractNum);
            return JObjectType.JContractIDKey;
        }

        /**
         * Returns the shard number.
         *
         * @return the shard number
         */
        public long getShardNum() {
            return shardNum;
        }

        /**
         * Returns the realm number.
         *
         * @return the realm number
         */
        public long getRealmNum() {
            return realmNum;
        }

        /**
         * Returns the contract number.
         *
         * @return the contract number
         */
        public long getContractNum() {
            return contractNum;
        }

        @Override
        public String toString() {
            return "<JContractID: " + shardNum + "." + realmNum + "." + contractNum + ">";
        }
    }

    /** An ECDSA secp384r1 public key. */
    public static class JECDSA_384Key extends JKey {

        /** The ECDSA secp384r1 public key bytes. */
        private final byte[] ECDSA_384Key;

        /**
         * Creates an ECDSA secp384r1 key.
         *
         * @param ECDSA_384Key the ECDSA secp384r1 public key bytes
         */
        public JECDSA_384Key(byte[] ECDSA_384Key) {
            this.ECDSA_384Key = ECDSA_384Key;
        }

        @Override
        protected JObjectType packTo(DataOutputStream out) throws IOException {
            out.write(ECDSA_384Key);
            return JObjectType.JECDSA_384Key;
        }

        @Override
        public String toString() {
            return "<JECDSA_384Key: ECDSA_384Key hex=" + HexFormat.of().formatHex(ECDSA_384Key) + ">";
        }

        /**
         * Returns the ECDSA secp384r1 public key bytes.
         *
         * @return the key bytes
         */
        public byte[] getECDSA384() {
            return ECDSA_384Key;
        }
    }

    /** A threshold key requiring a minimum number of signatures from a key list. */
    public static class JThresholdKey extends JKey {

        /** The minimum number of required signatures. */
        private final int threshold;
        /** The key list to choose signatures from. */
        private final JKeyList keys;

        /**
         * Creates a threshold key.
         *
         * @param keys the key list to choose signatures from
         * @param threshold the minimum number of required signatures
         */
        public JThresholdKey(JKeyList keys, int threshold) {
            this.keys = keys;
            this.threshold = threshold;
        }

        @Override
        protected JObjectType packTo(DataOutputStream out) throws IOException {
            out.writeInt(threshold);
            out.write(keys.serialize());
            return JObjectType.JThresholdKey;
        }

        @Override
        public String toString() {
            return "<JThresholdKey: thd=" + threshold + ", keys=" + keys.toString() + ">";
        }

        /**
         * Returns the key list to choose signatures from.
         *
         * @return the key list
         */
        public JKeyList getKeys() {
            return keys;
        }

        /**
         * Returns the minimum number of required signatures.
         *
         * @return the threshold
         */
        public int getThreshold() {
            return threshold;
        }
    }
}
