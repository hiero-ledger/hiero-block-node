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

package org.hiero.block.tools.states;

import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

public abstract class CryptoUtils {
	/** the type of hash to use */
	private final static String HASH_TYPE = "SHA-384";
	private final static String PRNG_TYPE = "SHA1PRNG";
	private final static String PRNG_PROVIDER = "SUN";

	// the algorithms and providers to use (AGR is key agreement, ENC is encryption, SIG is signatures)
	public final static String AGR_TYPE = "EC";
	public final static String AGR_PROVIDER = "SunEC";
	// final static ObjectIdentifier AGR_ALG_ID = AlgorithmId.sha384WithECDSA_oid;

	public final static String ENC_TYPE = "EC";
	public final static String ENC_PROVIDER = "SunEC";
	// final static ObjectIdentifier ENC_ALG_ID = AlgorithmId.sha384WithECDSA_oid;

	public final static String SIG_TYPE1 =  "RSA"; // or RSA or SHA384withRSA
	public final static String SIG_PROVIDER = "SunRsaSign";

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
		PublicKey publicKey = null;
		try {
			EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(bytes);
			KeyFactory keyFactory = KeyFactory.getInstance(keyType);
			publicKey = keyFactory.generatePublic(publicKeySpec);
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			throw new RuntimeException(e);
		}
		return publicKey;
	}

	// return the MessageDigest for the type of hash function used throughout the code
	public static MessageDigest getMessageDigest() {
		try {
			return MessageDigest.getInstance(HASH_TYPE);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create an instance of the default deterministic {@link SecureRandom}
	 *
	 * @return an instance of {@link SecureRandom}
	 * @throws NoSuchProviderException
	 * 		if the security provider is not available on the system
	 * @throws NoSuchAlgorithmException
	 * 		if the algorithm is not available on the system
	 */
	public static SecureRandom getDetRandom() throws NoSuchProviderException, NoSuchAlgorithmException {
		return SecureRandom.getInstance(PRNG_TYPE, PRNG_PROVIDER);
	}

}
