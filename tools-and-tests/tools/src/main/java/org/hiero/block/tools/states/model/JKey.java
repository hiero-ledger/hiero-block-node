// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;
import org.hiero.block.tools.states.utils.FCDataInputStream;

public class JKey {
    private static final long LEGACY_VERSION = 1;
    private static final long BPACK_VERSION = 2;

    public boolean hasEd25519Key() {
        return false;
    }

    public boolean hasECDSA_383Key() {
        return false;
    }

    public boolean hasRSA_3072Key() {
        return false;
    }

    public boolean hasKeyList() {
        return false;
    }

    public boolean hasThresholdKey() {
        return false;
    }

    public boolean hasContractID() {
        return false;
    }

    public JContractIDKey getContractIDKey() {
        return null;
    }

    public JThresholdKey getThresholdKey() {
        return null;
    }

    public JKeyList getKeyList() {
        return null;
    }

    public byte[] getEd25519() {
        return null;
    }

    public byte[] getECDSA384() {
        return null;
    }

    public byte[] getRSA3072() {
        return null;
    }

    public static <T extends JKey> T copyFrom(FCDataInputStream stream) throws IOException {
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

    public static <T> T deserialize(byte[] objectData) {
        return (T) deserialize((InputStream) (new ByteArrayInputStream(objectData)));
    }

    public static <T> T deserialize(InputStream inputStream) {

        try {
            ObjectInputStream in = new ObjectInputStream(inputStream);

            Object var3;
            try {
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
            return (T) var3;
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends JKey> T unpack(FCDataInputStream stream, JObjectType type, long length)
            throws IOException {

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

    public static class JEd25519Key extends JKey {
        private static final long serialVersionUID = 1L;
        private byte[] ed25519 = null;

        public JEd25519Key(byte[] ed25519) {
            this.ed25519 = ed25519;
        }

        @Override
        public String toString() {
            return "<JEd25519Key: ed25519 hex=" + HexFormat.of().formatHex(ed25519) + ">";
        }

        public byte[] getEd25519() {
            return ed25519;
        }

        public boolean hasEd25519Key() {
            return true;
        }
    }

    public static class JKeyList extends JKey {

        private static final long serialVersionUID = 1L;
        private List<JKey> keys = null;

        public JKeyList(List<JKey> keys) {
            this.keys = keys;
        }

        @Override
        public String toString() {
            return "<JKeyList: keys=" + keys.toString() + ">";
        }

        public boolean hasKeyList() {
            return true;
        }

        public List<JKey> getKeysList() {
            return keys;
        }

        public JKeyList getKeyList() {
            return this;
        }
    }

    public static class JRSA_3072Key extends JKey {

        private static final long serialVersionUID = 1L;
        private byte[] RSA_3072Key = null;

        public JRSA_3072Key(byte[] RSA_3072Key) {
            this.RSA_3072Key = RSA_3072Key;
        }

        @Override
        public String toString() {
            return "<JRSA_3072Key: RSA_3072Key hex=" + HexFormat.of().formatHex(RSA_3072Key) + ">";
        }

        public boolean hasRSA_3072Key() {
            return true;
        }

        public byte[] getRSA3072() {
            return RSA_3072Key;
        }
    }

    public static class JContractIDKey extends JKey {

        private static final long serialVersionUID = 1L;
        private long shardNum = 0; // the shard number (nonnegative)
        private long realmNum = 0; // the realm number (nonnegative)
        private long contractNum = 0; // a nonnegative number unique within its realm

        public JContractIDKey getContractIDKey() {
            return this;
        }

        public boolean hasContractID() {
            return true;
        }
        //
        //        public ContractID getContractID() {
        //            return ContractID.newBuilder().setShardNum(shardNum).setRealmNum(realmNum)
        //                    .setContractNum(contractNum).build();
        //        }

        public JContractIDKey(long shardNum, long realmNum, long contractNum) {
            super();
            this.shardNum = shardNum;
            this.realmNum = realmNum;
            this.contractNum = contractNum;
        }

        public long getShardNum() {
            return shardNum;
        }

        public long getRealmNum() {
            return realmNum;
        }

        public long getContractNum() {
            return contractNum;
        }

        @Override
        public String toString() {
            return "<JContractID: " + shardNum + "." + realmNum + "." + contractNum + ">";
        }
    }

    public static class JECDSA_384Key extends JKey {

        private static final long serialVersionUID = 1L;
        private byte[] ECDSA_384Key = null;

        public JECDSA_384Key(byte[] ECDSA_384Key) {
            this.ECDSA_384Key = ECDSA_384Key;
        }

        @Override
        public String toString() {
            return "<JECDSA_384Key: ECDSA_384Key hex=" + HexFormat.of().formatHex(ECDSA_384Key) + ">";
        }

        public boolean hasECDSA_384Key() {
            return true;
        }

        public byte[] getECDSA384() {
            return ECDSA_384Key;
        }
    }

    public static class JThresholdKey extends JKey {

        private static final long serialVersionUID = 1L;
        int threshold = 0;
        private JKeyList keys = null;

        public JThresholdKey(JKeyList keys, int threshold) {
            this.keys = keys;
            this.threshold = threshold;
        }

        @Override
        public String toString() {
            return "<JThresholdKey: thd=" + threshold + ", keys=" + keys.toString() + ">";
        }

        public boolean hasThresholdKey() {
            return true;
        }

        public JThresholdKey getThresholdKey() {
            return this;
        }

        public JKeyList getKeys() {
            return keys;
        }

        public int getThreshold() {
            return threshold;
        }
    }
}
