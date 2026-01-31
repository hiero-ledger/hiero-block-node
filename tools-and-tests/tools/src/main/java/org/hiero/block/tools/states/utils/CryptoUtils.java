// SPDX-License-Identifier: Apache-2.0
/*
 * (c) 2016-2019 Swirlds, Inc.
 *
 * This software is the confidential and proprietary information of
 * Swirlds, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Swirlds.
 *
 * SWIRLDS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SWIRLDS SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package org.hiero.block.tools.states.utils;

import com.hedera.hapi.node.base.ContractID;
import com.hedera.hapi.node.base.ContractID.ContractOneOfType;
import com.hedera.hapi.node.base.Key;
import com.hedera.hapi.node.base.KeyList;
import com.hedera.hapi.node.base.ThresholdKey;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import org.hiero.block.tools.states.model.JKey;
import org.hiero.block.tools.states.model.JKey.JKeyList;

/**
 * Utility functions for cryptographic operations, including public key serialization, message
 * digest creation, and conversion of legacy {@link JKey} types into their modern protobuf
 * {@link Key} equivalents.
 */
public abstract class CryptoUtils {
    /** The hash algorithm used throughout the platform (SHA-384). */
    private static final String HASH_TYPE = "SHA-384";

    /** Algorithm type for Elliptic Curve key agreement operations. */
    public static final String AGR_TYPE = "EC";

    /** Algorithm type for Elliptic Curve encryption operations. */
    public static final String ENC_TYPE = "EC";

    /** Algorithm type for RSA signature operations. */
    public static final String SIG_TYPE1 = "RSA";

    /**
     * Convert the given public key into a byte array, in a format that bytesToPublicKey can read.
     *
     * @param key
     * 		the public key to convert
     * @return a byte array representation of the public key
     */
    public static byte[] publicKeyToBytes(PublicKey key) {
        return key.getEncoded();
    }

    /**
     * Read a byte array created by publicKeyToBytes, and return the public key it represents
     *
     * @param bytes
     * 		the byte array from publicKeyToBytes
     * @param keyType
     * 		the type of key this is, such as SIG_TYPE
     * @return the public key represented by that byte array
     */
    public static PublicKey bytesToPublicKey(byte[] bytes, String keyType) {
        PublicKey publicKey;
        try {
            EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(bytes);
            KeyFactory keyFactory = KeyFactory.getInstance(keyType);
            publicKey = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
        return publicKey;
    }

    /**
     * Get a MessageDigest object for the hash type used in this code
     *
     * @return a MessageDigest object for the hash type used in this code
     */
    public static MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(HASH_TYPE);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a legacy {@link JKey} into a modern protobuf {@link Key}.
     *
     * <p>Supported {@link JKey} subtypes:
     * <ul>
     *   <li>{@link JKey.JEd25519Key} &rarr; {@link Key} with {@code ed25519} field</li>
     *   <li>{@link JKey.JECDSA_384Key} &rarr; {@link Key} with {@code ecdsa384} field</li>
     *   <li>{@link JKey.JRSA_3072Key} &rarr; {@link Key} with {@code rsa3072} field</li>
     *   <li>{@link JKey.JContractIDKey} &rarr; {@link Key} with {@code contractID} field</li>
     *   <li>{@link JKey.JKeyList} &rarr; {@link Key} with {@code keyList} field</li>
     *   <li>{@link JKey.JThresholdKey} &rarr; {@link Key} with {@code thresholdKey} field</li>
     * </ul>
     *
     * @param <K> the specific {@link JKey} subtype
     * @param jKey the legacy key to convert
     * @return the equivalent protobuf {@link Key}
     * @throws IllegalArgumentException if the {@link JKey} subtype is not recognized
     */
    public static <K extends JKey> Key convertKey(K jKey) {
        return switch (jKey) {
            case JKey.JThresholdKey thresholdKey ->
                Key.newBuilder()
                        .thresholdKey(
                                new ThresholdKey(thresholdKey.getThreshold(), convertKeyList(thresholdKey.getKeys())))
                        .build();
            case JKey.JEd25519Key ed25519Key ->
                Key.newBuilder().ed25519(Bytes.wrap(ed25519Key.getEd25519())).build();
            case JKey.JECDSA_384Key ed384Key ->
                Key.newBuilder().ecdsa384(Bytes.wrap(ed384Key.getECDSA384())).build();
            case JKey.JRSA_3072Key jrsa3072Key ->
                Key.newBuilder().rsa3072(Bytes.wrap(jrsa3072Key.getRSA3072())).build();
            case JKey.JContractIDKey contractIDKey ->
                Key.newBuilder()
                        .contractID(new ContractID(
                                contractIDKey.getShardNum(),
                                contractIDKey.getRealmNum(),
                                new OneOf<>(ContractOneOfType.CONTRACT_NUM, contractIDKey.getContractNum())))
                        .build();
            case JKeyList keyList ->
                Key.newBuilder().keyList(convertKeyList(keyList)).build();
            default ->
                throw new IllegalArgumentException(
                        "Unsupported JKey type: " + jKey.getClass().getSimpleName());
        };
    }

    /**
     * Converts a legacy {@link JKeyList} into a protobuf {@link KeyList} by recursively converting
     * each contained {@link JKey} via {@link #convertKey(JKey)}.
     *
     * @param jKeyList the legacy key list to convert
     * @return the equivalent protobuf {@link KeyList}
     */
    static KeyList convertKeyList(JKeyList jKeyList) {
        return new KeyList(
                jKeyList.getKeysList().stream().map(CryptoUtils::convertKey).toList());
    }
}
