package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.CryptoUtils;
import org.hiero.block.tools.states.Utilities;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.PublicKey;

public record Address(
        long id,
        String nickname,
        String selfName,
        long stake,
        boolean ownHost,
        byte[] addressInternalIpv4, int portInternalIpv4,
        byte[] addressExternalIpv4, int portExternalIpv4,
        byte[] addressInternalIpv6, int portInternalIpv6,
        byte[] addressExternalIpv6, int portExternalIpv6,
        PublicKey sigPublicKey,
        PublicKey encPublicKey,
        PublicKey agreePublicKey,
        String memo
) {
    private static final byte[] ALL_INTERFACES = new byte[] { 0, 0, 0, 0 };
    private static final int MAX_KEY_LENGTH = 6_144;

    public void updateHash(MessageDigest md) {
        // UTF-8 is supported by all Java implementations
        Hash.update(md, id);
        md.update(Utilities.getNormalisedStringBytes(nickname));
        md.update(Utilities.getNormalisedStringBytes(selfName));
        Hash.update(md, stake);
        // ownHost should not be included because different platforms will have different hashes
        if (addressInternalIpv4 != null) {
            md.update(addressInternalIpv4);
            Hash.update(md, portInternalIpv4);
        }
        if (addressExternalIpv4 != null) {
            md.update(addressExternalIpv4);
            Hash.update(md, portExternalIpv4);
        }
        if (addressInternalIpv6 != null) {
            md.update(addressInternalIpv6);
            Hash.update(md, portInternalIpv6);
        }
        if (addressExternalIpv6 != null) {
            md.update(addressExternalIpv6);
            Hash.update(md, portExternalIpv6);
        }

        md.update(CryptoUtils.publicKeyToBytes(sigPublicKey));
        md.update(CryptoUtils.publicKeyToBytes(encPublicKey));
        md.update(CryptoUtils.publicKeyToBytes(agreePublicKey));
        // XXX add memo?
    }

    private enum KeyType {
        SIG,
        AGR,
        ENC;
    }

    /**
     * Return a new Address object read from the given stream. It should have been written to the stream
     * with writeAddress().
     *
     * @param inStream
     * 		the stream to read from
     * @return the new Address object that was read.
     */
    public static Address readAddress(DataInputStream inStream) throws IOException {
        return new Address(//
                inStream.readLong(), // id
                Utilities.readNormalisedString(inStream), // nickname
                Utilities.readNormalisedString(inStream), // selfName
                inStream.readLong(), // stake
                false, // ownHost
                // XXX ownHost needs to be set for each node when being read
                readBytes(inStream), inStream.readInt(), // addressInternalIpv4 portInternalIpv4
                readBytes(inStream), inStream.readInt(), // addressExternalIpv4 portExternalIpv4
                readBytes(inStream), inStream.readInt(), // addressInternalIpv6 portInternalIpv6
                readBytes(inStream), inStream.readInt(), // addressExternalIpv6 portExternalIpv6
                CryptoUtils.bytesToPublicKey(readBytes(inStream),
                        CryptoUtils.SIG_TYPE1), // sigPublicKey
                CryptoUtils.bytesToPublicKey(readBytes(inStream),
                        CryptoUtils.ENC_TYPE), // encPublicKey
                CryptoUtils.bytesToPublicKey(readBytes(inStream),
                        CryptoUtils.AGR_TYPE),  // agreePublicKey
                Utilities.readNormalisedString(inStream)); // memo
    }

    public void writeAddress(DataOutputStream outStream) throws IOException {
        outStream.writeLong(id);
        Utilities.writeNormalisedString(outStream, nickname);
        Utilities.writeNormalisedString(outStream, selfName);
        outStream.writeLong(stake);
        // this should not be written because it can differ on different nodes
        // outStream.writeBoolean(ownHost);
        writeBytes(outStream, addressInternalIpv4);
        outStream.writeInt(portInternalIpv4);
        writeBytes(outStream, addressExternalIpv4);
        outStream.writeInt(portExternalIpv4);
        writeBytes(outStream, addressInternalIpv6);
        outStream.writeInt(portInternalIpv6);
        writeBytes(outStream, addressExternalIpv6);
        outStream.writeInt(portExternalIpv6);
        writeBytes(outStream, CryptoUtils.publicKeyToBytes(sigPublicKey));
        writeBytes(outStream, CryptoUtils.publicKeyToBytes(encPublicKey));
        writeBytes(outStream, CryptoUtils.publicKeyToBytes(agreePublicKey));
        Utilities.writeNormalisedString(outStream, memo);
    }

    private static void writeBytes(DataOutputStream outStream, byte[] data)
            throws IOException {
        if (data == null) {
            outStream.writeInt(-1);
        } else {
            outStream.writeInt(data.length);
            outStream.write(data);
        }
    }

    private static byte[] readBytes(DataInputStream inStream)
            throws IOException {
        int len = inStream.readInt();
        if (len < 0) {
            // if length is negative, it's a null value
            return null;
        }
        byte[] bytes = new byte[len];
        inStream.readFully(bytes);
        return bytes;
    }

    @Override
    public String toString() {
        return "Address[" +
                "id=" + id +
                ", nickname='" + nickname + '\'' +
                ", selfName='" + selfName + '\'' +
                ", stake=" + stake +
                ", ownHost=" + ownHost +
                ", addressInternalIpv4=" + formatIpv4(addressInternalIpv4) +
                ", portInternalIpv4=" + portInternalIpv4 +
                ", addressExternalIpv4=" + formatIpv4(addressExternalIpv4) +
                ", portExternalIpv4=" + portExternalIpv4 +
                ", addressInternalIpv6=" + formatIpv4(addressInternalIpv6) +
                ", portInternalIpv6=" + portInternalIpv6 +
                ", addressExternalIpv6=" + formatIpv4(addressExternalIpv6) +
                ", portExternalIpv6=" + portExternalIpv6 +
                ", sigPublicKey=" + sigPublicKey +
                ", encPublicKey=" + encPublicKey +
                ", agreePublicKey=" + agreePublicKey +
                ", memo='" + memo + '\'' +
                ']';
    }

    public static String formatIpv4(byte[] ipV4Address) {
        if (ipV4Address == null || ipV4Address.length != 4) {
            return "EMPTY";
        }
        int part1 = ipV4Address[0] & 0xFF;
        int part2 = ipV4Address[1] & 0xFF;
        int part3 = ipV4Address[2] & 0xFF;
        int part4 = ipV4Address[3] & 0xFF;
        return String.format("%d.%d.%d.%d", part1, part2, part3, part4);
    }
}
